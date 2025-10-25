#!/usr/bin/env python3
"""
Optimized runner: Parse JSON queries, use planner to choose MV, execute, output CSV + report.
"""
import argparse
import time
import json
import duckdb
import orjson
from pathlib import Path
from planner import choose_plan


def lit(col, val):
    """Convert value to SQL literal with type awareness."""
    # Type inference based on column name - use DATE/TIMESTAMP constructors
    if col in ("day", "week", "hour"):
        if col == "day":
            return f"DATE '{val}'"
        return f"TIMESTAMP '{val}'"
    if isinstance(val, (int, float)):
        return str(val)
    # String literal
    sval = str(val).replace("'", "''")
    return f"'{sval}'"



def build_where(where, keep_cols):
    """Build WHERE clause from JSON filters, keeping only specified columns."""
    parts = []
    for c in where or []:
        # If keep_cols is specified (not None) and not empty, filter by it
        # If keep_cols is empty list, skip all filters
        if keep_cols is not None and c["col"] not in keep_cols:
            continue
        
        op = c["op"]
        col = c["col"]
        # Cast day column from VARCHAR (partition column) to DATE for comparisons
        col_expr = f"CAST({col} AS DATE)" if col == "day" else col
        
        if op in ("eq", "neq"):
            sym = "=" if op == "eq" else "!="
            parts.append(f"{col_expr} {sym} {lit(col, c['val'])}")
        elif op in ("lt", "lte", "gt", "gte"):
            sym = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[op]
            parts.append(f"{col_expr} {sym} {lit(col, c['val'])}")
        elif op == "between":
            lo, hi = c["val"]
            # Handle date literals consistently
            lo_lit = lit(col, lo)
            hi_lit = lit(col, hi)
            parts.append(f"{col_expr} BETWEEN {lo_lit} AND {hi_lit}")
        elif op == "in":
            vals = ", ".join(lit(col, v) for v in c["val"])
            parts.append(f"{col_expr} IN ({vals})")
    
    return ("WHERE " + " AND ".join(parts)) if parts else ""


def build_select(select_items, table=None):
    """Build SELECT clause from JSON select list."""
    parts = []
    for item in select_items:
        if isinstance(item, str):
            parts.append(item)
        else:
            # Aggregate function: {"SUM": "bid_price"}
            for func, col in item.items():
                # Map to pre-aggregated columns in MVs
                if func == "SUM" and col == "bid_price":
                    if table in ("mv_day_impr_revenue", "mv_day_country_publisher_impr", "mv_day_minute_impr"):
                        expr = "SUM(sum_bid)"
                        parts.append(f'{expr} AS "{func.upper()}({col})"')
                    else:
                        expr = f"{func.upper()}({col})"
                        parts.append(f'{expr} AS "{func.upper()}({col})"')
                elif func == "AVG" and col == "total_price" and table == "mv_country_purchase_avg":
                    # Use pre-computed average directly (no aggregation needed)
                    parts.append(f'avg_total AS "{func.upper()}({col})"')
                elif func == "COUNT" and table == "mv_adv_type_counts":
                    # Re-aggregate counts
                    parts.append(f'SUM(n) AS "{func.upper()}({col})"')
                else:
                    expr = f"{func.upper()}({col})"
                    parts.append(f'{expr} AS "{func.upper()}({col})"')
    return ", ".join(parts) if parts else "*"


def run_query(con, q, out_path):
    """Execute a JSON query and write results to CSV."""
    table, proj = choose_plan(q)
    keep = proj.get("keep_where", [])
    
    # Build SQL
    select_sql = build_select(q.get("select", []), table)
    where_sql = build_where(q.get("where"), keep)
    
    # Use GROUP BY when MV needs re-aggregation (has finer granularity than query)
    # Skip for exact-match MVs where pre-aggregated values are selected directly
    group_by = q.get("group_by", [])
    
    # These MVs return pre-aggregated results at exact query granularity
    skip_group_by_for = ["mv_country_purchase_avg"]
    
    if table in skip_group_by_for:
        group_sql = ""
    else:
        group_sql = f"GROUP BY {', '.join(group_by)}" if group_by else ""
    
    sql = f"SELECT {select_sql} FROM {table} {where_sql} {group_sql}".strip()
    
    # Debug: print SQL for troubleshooting
    # print(f"\n[DEBUG] SQL: {sql}\n")
    
    # Execute and time
    t0 = time.perf_counter()
    con.execute(f"COPY ({sql}) TO '{out_path}' WITH (HEADER, DELIMITER ',');")
    dt = time.perf_counter() - t0
    
    return table, dt


def main():
    ap = argparse.ArgumentParser(description="Execute JSON queries with MV optimization")
    ap.add_argument("--lake", required=True, help="Path to Parquet lake")
    ap.add_argument("--mvs", required=True, help="Path to materialized views")
    ap.add_argument("--queries", required=True, help="Directory of JSON query files")
    ap.add_argument("--out", required=True, help="Output directory for results")
    ap.add_argument("--threads", type=int, default=8, help="DuckDB thread count")
    ap.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[runner] Connecting to DuckDB (threads={args.threads}, mem={args.mem})")
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={args.threads};")
        con.execute(f"SET memory_limit='{args.mem}';")
        con.execute(f"PRAGMA temp_directory='{(out_dir / '.ducktmp').absolute()}';")
    except duckdb.Error:
        pass

    # Create events view on Parquet lake
    print(f"[runner] Loading events view from {args.lake}")
    con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet('{args.lake}/events/day=*/**/*.parquet');")

    # Load available MVs into memory as tables
    print(f"[runner] Loading materialized views from {args.mvs}")
    mv_names = [
        "mv_day_impr_revenue",
        "mv_day_country_publisher_impr",
        "mv_country_purchase_avg",
        "mv_adv_type_counts",
        "mv_day_minute_impr"
    ]
    available = set()
    for name in mv_names:
        mv_path = Path(args.mvs) / name
        if mv_path.exists():
            # Handle both partitioned (directory) and non-partitioned (file) MVs
            if mv_path.is_dir():
                pattern = f"{mv_path}/**/*.parquet"
            else:
                pattern = str(mv_path)
            
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_parquet('{pattern}');")
            try:
                con.execute(f"ANALYZE {name};")
            except duckdb.Error:
                pass
            available.add(name)
            print(f"  ✓ {name}")

    # Run queries
    print(f"\n[runner] Executing queries from {args.queries}")
    metrics = []
    query_files = sorted(Path(args.queries).glob("*.json"))
    
    for i, p in enumerate(query_files, 1):
        q = orjson.loads(p.read_bytes())
        out_file = out_dir / f"{p.stem}.csv"
        table, secs = run_query(con, q, out_file)
        print(f"  [{i}/{len(query_files)}] {p.name} -> {out_file.name} [{table}] in {secs:.4f}s")
        metrics.append({
            "query": p.name,
            "table": table,
            "seconds": round(secs, 4),
            "output": str(out_file)
        })

    # Write report
    report_path = out_dir / "report.json"
    report_path.write_text(json.dumps({"queries": metrics}, indent=2))
    print(f"\n✓ Report written to {report_path}")


if __name__ == "__main__":
    main()
