#!/usr/bin/env python3
"""
Adaptive runner with:
1. Fuzzy MV matching for wider MVs
2. Sampling for ad-hoc queries
3. Query pattern tracking and MV suggestions
"""
import argparse
import time
import json
import duckdb
import orjson
from pathlib import Path
from planner_adaptive import choose_plan_adaptive
from mv_analyzer import MVAnalyzer


def lit(col, val):
    """Convert value to SQL literal with type awareness."""
    if col in ("day", "week", "hour"):
        if col == "day":
            return f"DATE '{val}'"
        return f"TIMESTAMP '{val}'"
    if isinstance(val, (int, float)):
        return str(val)
    sval = str(val).replace("'", "''")
    return f"'{sval}'"


def build_where(where, keep_cols):
    """Build WHERE clause from JSON filters."""
    parts = []
    for c in where or []:
        if keep_cols is not None and c["col"] not in keep_cols:
            continue
        
        op = c["op"]
        col = c["col"]
        col_expr = f"CAST({col} AS DATE)" if col == "day" else col
        
        if op in ("eq", "neq"):
            sym = "=" if op == "eq" else "!="
            parts.append(f"{col_expr} {sym} {lit(col, c['val'])}")
        elif op in ("lt", "lte", "gt", "gte"):
            sym = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[op]
            parts.append(f"{col_expr} {sym} {lit(col, c['val'])}")
        elif op == "between":
            lo, hi = c["val"]
            parts.append(f"{col_expr} BETWEEN {lit(col, lo)} AND {lit(col, hi)}")
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
            for func, col in item.items():
                # Map to pre-aggregated columns in MVs
                if func == "SUM" and col == "bid_price":
                    if "mv_" in (table or "") and table != "mv_country_purchase_avg":
                        expr = "SUM(sum_bid)"
                        parts.append(f'{expr} AS "{func.upper()}({col})"')
                    else:
                        expr = f"{func.upper()}({col})"
                        parts.append(f'{expr} AS "{func.upper()}({col})"')
                elif func == "AVG" and col == "total_price" and table == "mv_country_purchase_avg":
                    parts.append(f'avg_total AS "{func.upper()}({col})"')
                elif func == "COUNT" and "mv_adv_type_counts" in (table or ""):
                    parts.append(f'SUM(n) AS "{func.upper()}({col})"')
                elif func == "COUNT" and "mv_" in (table or ""):
                    parts.append(f'SUM(events) AS "{func.upper()}({col})"')
                else:
                    expr = f"{func.upper()}({col})"
                    parts.append(f'{expr} AS "{func.upper()}({col})"')
    return ", ".join(parts) if parts else "*"


def run_query_adaptive(con, q, out_path, available_mvs, analyzer=None):
    """Execute a JSON query with adaptive planning."""
    table, plan = choose_plan_adaptive(q, available_mvs)
    
    keep = plan.get("keep_where", [])
    match_type = plan.get("match_type", "unknown")
    sample_rate = plan.get("sample_rate")
    
    # Build SQL
    select_sql = build_select(q.get("select", []), table)
    where_sql = build_where(q.get("where"), keep)
    
    # Apply sampling if recommended
    table_ref = table
    if sample_rate and table == "events_v":
        table_ref = f"(SELECT * FROM {table} USING SAMPLE {int(sample_rate*100)}%)"
    
    # Handle re-aggregation for partial matches
    group_by = q.get("group_by", [])
    needs_reagg = plan.get("needs_reaggregate", False)
    
    if needs_reagg or (match_type in ["partial", "full_scan"] and group_by):
        group_sql = f"GROUP BY {', '.join(group_by)}" if group_by else ""
    else:
        group_sql = ""
    
    sql = f"SELECT {select_sql} FROM {table_ref} {where_sql} {group_sql}".strip()
    
    # Execute and time
    t0 = time.perf_counter()
    con.execute(f"COPY ({sql}) TO '{out_path}' WITH (HEADER, DELIMITER ',');")
    dt = time.perf_counter() - t0
    
    # Record in analyzer
    if analyzer:
        analyzer.record_query(q, hit_mv=table, exec_time=dt)
    
    return {
        "table": table,
        "match_type": match_type,
        "seconds": dt,
        "approximate": plan.get("approximate", False),
        "score": plan.get("score", 0)
    }


def main():
    ap = argparse.ArgumentParser(description="Adaptive query runner with MV optimization and sampling")
    ap.add_argument("--lake", required=True, help="Path to Parquet lake")
    ap.add_argument("--mvs", required=True, help="Path to materialized views")
    ap.add_argument("--queries", required=True, help="Directory of JSON query files")
    ap.add_argument("--out", required=True, help="Output directory for results")
    ap.add_argument("--threads", type=int, default=8, help="DuckDB thread count")
    ap.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    ap.add_argument("--analyze", action="store_true", help="Generate MV suggestions report")
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

    # Load available MVs
    print(f"[runner] Loading materialized views from {args.mvs}")
    mv_names = [
        "mv_day_impr_revenue",
        "mv_day_country_publisher_impr",
        "mv_country_purchase_avg",
        "mv_adv_type_counts",
        "mv_day_minute_impr",
        # Wider MVs
        "mv_hour_country_pub_adv_impr",
        "mv_day_country_type_adv",
        "mv_week_adv_country_type"
    ]
    
    available = []
    for name in mv_names:
        mv_path = Path(args.mvs) / name
        if mv_path.exists():
            if mv_path.is_dir():
                pattern = f"{mv_path}/**/*.parquet"
            else:
                pattern = str(mv_path)
            
            try:
                con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_parquet('{pattern}');")
                con.execute(f"ANALYZE {name};")
                available.append(name)
                print(f"  ✓ {name}")
            except Exception as e:
                print(f"  ✗ {name} - {e}")

    # Initialize analyzer
    analyzer = MVAnalyzer() if args.analyze else None

    # Run queries
    print(f"\n[runner] Executing queries from {args.queries}")
    metrics = []
    query_files = sorted(Path(args.queries).glob("*.json"))
    
    for i, p in enumerate(query_files, 1):
        q = orjson.loads(p.read_bytes())
        out_file = out_dir / f"{p.stem}.csv"
        
        result = run_query_adaptive(con, q, out_file, available, analyzer)
        
        match_icon = {"exact": "🎯", "partial": "⚡", "sampling": "🎲", "full_scan": "📊"}.get(result["match_type"], "❓")
        
        print(f"  [{i}/{len(query_files)}] {p.name} -> {out_file.name}")
        print(f"      {match_icon} {result['table']} ({result['match_type']}) in {result['seconds']:.4f}s")
        if result.get("approximate"):
            print(f"      ⚠️  Approximate result (sampled)")
        
        metrics.append({
            "query": p.name,
            "table": result["table"],
            "match_type": result["match_type"],
            "seconds": round(result["seconds"], 4),
            "approximate": result.get("approximate", False),
            "score": result.get("score", 0),
            "output": str(out_file)
        })

    # Write main report
    report_path = out_dir / "report_adaptive.json"
    report = {
        "queries": metrics,
        "summary": {
            "total_queries": len(metrics),
            "mv_hits": sum(1 for m in metrics if m["table"] != "events_v"),
            "full_scans": sum(1 for m in metrics if m["match_type"] == "full_scan"),
            "sampling": sum(1 for m in metrics if m["match_type"] == "sampling"),
            "total_time": sum(m["seconds"] for m in metrics)
        }
    }
    report_path.write_text(json.dumps(report, indent=2))
    print(f"\n✓ Report written to {report_path}")

    # Generate MV suggestions if requested
    if analyzer:
        suggestions_path = out_dir / "mv_suggestions.json"
        analyzer.generate_report(str(suggestions_path))
        print(f"✓ MV suggestions written to {suggestions_path}")
        
        # Print top suggestions
        suggestions = analyzer.suggest_mvs(min_frequency=1, max_suggestions=3)
        if suggestions:
            print(f"\n{'='*60}")
            print("🔍 TOP MV SUGGESTIONS")
            print(f"{'='*60}")
            for i, sug in enumerate(suggestions, 1):
                print(f"\n{i}. Dimensions: {sug['dimensions']}")
                print(f"   Frequency: {sug['frequency']} queries")
                print(f"   Benefit Score: {sug['estimated_benefit']:.0f}/100")


if __name__ == "__main__":
    main()
