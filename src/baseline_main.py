#!/usr/bin/env python3
"""
Baseline runner: Execute JSON queries directly on events view (no MV optimization).
"""
import duckdb
import time
import csv
import json
import orjson
import argparse
from pathlib import Path
from assembler import assemble_sql


def load_events_view(con, data_dir: Path, mode="csv"):
    """Load events into a view from CSV or Parquet."""
    if mode == "csv":
        print(f"[baseline] Loading events from CSV in {data_dir}")
        con.execute(f"""
        CREATE OR REPLACE VIEW events AS
        WITH raw AS (
          SELECT * FROM read_csv_auto('{data_dir}/events*.csv', union_by_name=true, normalize_names=true)
        ), casted AS (
          SELECT
            to_timestamp(CAST(ts as DOUBLE)/1000.0) AS ts,
            DATE_TRUNC('week', to_timestamp(CAST(ts as DOUBLE)/1000.0)) AS week,
            DATE(to_timestamp(CAST(ts as DOUBLE)/1000.0)) AS day,
            DATE_TRUNC('hour', to_timestamp(CAST(ts as DOUBLE)/1000.0)) AS hour,
            STRFTIME(to_timestamp(CAST(ts as DOUBLE)/1000.0), '%Y-%m-%d %H:%M') AS minute,
            type, auction_id,
            TRY_CAST(advertiser_id AS INTEGER) AS advertiser_id,
            TRY_CAST(publisher_id  AS INTEGER) AS publisher_id,
            NULLIF(bid_price,'')::DOUBLE AS bid_price,
            TRY_CAST(user_id AS BIGINT) AS user_id,
            NULLIF(total_price,'')::DOUBLE AS total_price,
            country
          FROM raw
        )
        SELECT * FROM casted;
        """)
    else:
        print(f"[baseline] Loading events from Parquet lake in {data_dir}")
        con.execute(f"CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('{data_dir}/events/day=*/**/*.parquet');")


def main():
    ap = argparse.ArgumentParser(description="Baseline runner without MV optimization")
    ap.add_argument("--data-dir", required=True, help="Path to data directory")
    ap.add_argument("--out-dir", required=True, help="Output directory for results")
    ap.add_argument("--queries", required=True, help="Directory of JSON query files")
    ap.add_argument("--mode", choices=["csv", "lake"], default="csv", help="Data source mode")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    
    print("[baseline] Connecting to DuckDB")
    con = duckdb.connect("baseline.duckdb")

    load_events_view(con, Path(args.data_dir), mode=args.mode)

    results = []
    query_files = sorted(Path(args.queries).glob("*.json"))
    print(f"\n[baseline] Executing {len(query_files)} queries")
    
    for i, p in enumerate(query_files, 1):
        q = orjson.loads(p.read_bytes())
        sql = assemble_sql(q)
        
        t0 = time.time()
        res = con.execute(sql)
        cols = [d[0] for d in res.description]
        rows = res.fetchall()
        dt = time.time() - t0
        
        out_file = out / f"{p.stem}.csv"
        with out_file.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        
        print(f"  [{i}/{len(query_files)}] {p.name}: {dt:.4f}s rows={len(rows)}")
        results.append({"query": p.name, "seconds": round(dt, 4), "rows": len(rows)})
    
    report_path = out / "baseline_report.json"
    report_path.write_text(json.dumps({"queries": results}, indent=2))
    print(f"\n✓ Baseline report written to {report_path}")


if __name__ == "__main__":
    main()
