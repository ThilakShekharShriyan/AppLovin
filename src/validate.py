#!/usr/bin/env python3
"""
Validate MV correctness by comparing MV results against full scan results.
"""
import argparse
import duckdb
import hashlib
import orjson
import pandas as pd
from pathlib import Path
from planner import choose_plan
from runner import build_select, build_where


def hash_df(df):
    """Hash a dataframe for comparison."""
    csv = df.to_csv(index=False, header=True)
    return len(df), hashlib.sha256(csv.encode()).hexdigest()


def main():
    ap = argparse.ArgumentParser(description="Validate MV correctness")
    ap.add_argument("--lake", required=True, help="Path to Parquet lake")
    ap.add_argument("--mvs", required=True, help="Path to materialized views")
    ap.add_argument("--queries", required=True, help="Directory of JSON query files")
    args = ap.parse_args()

    print("[validate] Connecting to DuckDB")
    con = duckdb.connect(":memory:")
    
    # Load events view
    print(f"[validate] Loading events view from {args.lake}")
    con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet('{args.lake}/events/day=*/**/*.parquet');")
    
    # Load MVs
    print(f"[validate] Loading materialized views from {args.mvs}")
    for name in ["mv_day_impr_revenue", "mv_day_country_publisher_impr", "mv_country_purchase_avg", "mv_adv_type_counts", "mv_day_minute_impr"]:
        p = Path(args.mvs) / name
        if p.exists():
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_parquet('{p}/**/*.parquet');")
            print(f"  ✓ {name}")

    ok = diff = err = 0
    query_files = sorted(Path(args.queries).glob("*.json"))
    print(f"\n[validate] Checking {len(query_files)} queries")
    
    for f in query_files:
        q = orjson.loads(f.read_bytes())
        table, proj = choose_plan(q)
        
        try:
            # Build SQL for MV path
            select_sql = build_select(q.get("select", []))
            where_sql = build_where(q.get("where"), proj.get("keep_where", []))
            group_by = q.get("group_by", [])
            group_sql = f"GROUP BY {', '.join(group_by)}" if group_by else ""
            sql_mv = f"SELECT {select_sql} FROM {table} {where_sql} {group_sql}".strip()
            
            # Build SQL for events_v path (full scan)
            sql_full = f"SELECT {select_sql} FROM events_v {build_where(q.get('where'), None)} {group_sql}".strip()
            
            # Execute both
            df_mv = con.execute(sql_mv).df().sort_values(by=list(df_mv.columns)).reset_index(drop=True) if len(con.execute(sql_mv).df()) > 0 else con.execute(sql_mv).df()
            df_full = con.execute(sql_full).df().sort_values(by=list(df_full.columns)).reset_index(drop=True) if len(con.execute(sql_full).df()) > 0 else con.execute(sql_full).df()
            
            # Compare
            if df_mv.equals(df_full):
                print(f"  ✓ {f.name} ({table})")
                ok += 1
            else:
                print(f"  ✗ {f.name} DIFF ({table} vs events_v)")
                diff += 1
        except Exception as e:
            print(f"  ✗ {f.name} ERROR: {e}")
            err += 1
    
    print(f"\n[validate] OK={ok} DIFF={diff} ERR={err}")
    if diff > 0 or err > 0:
        exit(1)


if __name__ == "__main__":
    main()
