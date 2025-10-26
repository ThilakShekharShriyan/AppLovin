#!/usr/bin/env python3
"""
Benchmark comparison: Run identical queries across DuckDB (MVs) and DuckDB (full scan).
"""
import argparse
import json
import time
from pathlib import Path
import duckdb
from planner import choose_plan


def run_duckdb_mv_queries(lake_path, mvs_path, queries_dir, con):
    """Run queries using DuckDB with MVs."""
    from runner import run_query
    
    queries_dir = Path(queries_dir)
    query_files = sorted(queries_dir.glob("*.json"))
    
    print(f"\n[benchmark] === DuckDB with MVs ({len(query_files)} queries) ===")
    
    # Load MVs
    print("[benchmark] Loading MVs...")
    mv_names = [
        "mv_day_impr_revenue",
        "mv_day_country_publisher_impr",
        "mv_country_purchase_avg",
        "mv_adv_type_counts",
        "mv_day_minute_impr",
        "mv_hour_country_pub_adv_impr",
        "mv_day_country_type_adv"
    ]
    
    for name in mv_names:
        mv_path = Path(mvs_path) / name
        if mv_path.exists():
            pattern = f"{mv_path}/**/*.parquet" if mv_path.is_dir() else str(mv_path)
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_parquet('{pattern}');")
    
    results = []
    for qfile in query_files:
        query_json = json.loads(qfile.read_text())
        
        # Determine which table/MV would be used
        table, proj = choose_plan(query_json)
        
        # Build and execute query (simplified version of runner.py logic)
        try:
            from runner import build_select, build_where
            
            select_sql = build_select(query_json.get("select", []), table)
            where_sql = build_where(query_json.get("where"), proj.get("keep_where", []))
            group_by = query_json.get("group_by", [])
            group_sql = f"GROUP BY {', '.join(group_by)}" if group_by and table != "mv_country_purchase_avg" else ""
            
            sql = f"SELECT {select_sql} FROM {table} {where_sql} {group_sql}".strip()
            
            start = time.time()
            result = con.execute(sql).fetchall()
            elapsed = time.time() - start
            
            print(f"  ✓ {qfile.name}: {elapsed:.4f}s using {table}")
            
            results.append({
                "query": qfile.name,
                "table": table,
                "seconds": elapsed,
                "rows": len(result)
            })
            
        except Exception as e:
            print(f"  ✗ {qfile.name}: {e}")
            results.append({
                "query": qfile.name,
                "error": str(e)
            })
    
    return results


def run_duckdb_full_scan_queries(lake_path, queries_dir, con):
    """Run queries using DuckDB without MVs (full scan on events_v)."""
    queries_dir = Path(queries_dir)
    query_files = sorted(queries_dir.glob("*.json"))
    
    print(f"\n[benchmark] === DuckDB Full Scan ({len(query_files)} queries) ===")
    
    # Create events view (if not exists)
    con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet('{lake_path}/events/day=*/**/*.parquet');")
    
    results = []
    for qfile in query_files:
        query_json = json.loads(qfile.read_text())
        
        try:
            from assembler import assemble_sql
            
            # Build SQL for full scan
            query_json["from"] = "events_v"
            sql = assemble_sql(query_json)
            
            start = time.time()
            result = con.execute(sql).fetchall()
            elapsed = time.time() - start
            
            print(f"  ✓ {qfile.name}: {elapsed:.4f}s")
            
            results.append({
                "query": qfile.name,
                "table": "events_v",
                "seconds": elapsed,
                "rows": len(result)
            })
            
        except Exception as e:
            print(f"  ✗ {qfile.name}: {e}")
            results.append({
                "query": qfile.name,
                "error": str(e)
            })
    
    return results


def generate_comparison_report(duckdb_mv_results, duckdb_full_results, output_path):
    """Generate comparison report for DuckDB MV vs DuckDB full scan."""
    # Aggregate by query
    queries = {}
    for r in duckdb_mv_results:
        qname = r["query"]
        queries.setdefault(qname, {})["duckdb_mv"] = r
    for r in duckdb_full_results:
        qname = r["query"]
        queries.setdefault(qname, {})["duckdb_full"] = r

    # Build comparison table
    comparison = []
    for qname, engines in sorted(queries.items()):
        mv_time = engines.get("duckdb_mv", {}).get("seconds")
        full_time = engines.get("duckdb_full", {}).get("seconds")
        speedup_vs_full = full_time / mv_time if mv_time and full_time else None
        comparison.append({
            "query": qname,
            "duckdb_mv": {"seconds": mv_time, "table": engines.get("duckdb_mv", {}).get("table")},
            "duckdb_full": {"seconds": full_time, "table": "events_v"},
            "speedup": {"mv_vs_full": round(speedup_vs_full, 2) if speedup_vs_full else None}
        })

    # Summary
    summary = {
        "duckdb_mv": {
            "total_time": sum(r.get("seconds", 0) for r in duckdb_mv_results),
            "avg_time": sum(r.get("seconds", 0) for r in duckdb_mv_results) / len(duckdb_mv_results) if duckdb_mv_results else 0,
            "queries": len(duckdb_mv_results),
        },
        "duckdb_full": {
            "total_time": sum(r.get("seconds", 0) for r in duckdb_full_results),
            "avg_time": sum(r.get("seconds", 0) for r in duckdb_full_results) / len(duckdb_full_results) if duckdb_full_results else 0,
            "queries": len(duckdb_full_results),
        },
    }

    report = {
        "summary": summary,
        "queries": comparison,
        "raw_results": {"duckdb_mv": duckdb_mv_results, "duckdb_full": duckdb_full_results},
    }

    # Save report
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n[benchmark] ✓ Comparison report saved to {output_path}")

    # Print summary table
    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)
    print(f"{'Engine':<20} {'Total Time (s)':<15} {'Avg Time (s)':<15} {'Queries':<10}")
    print("-" * 80)
    for engine in ["duckdb_mv", "duckdb_full"]:
        stats = summary[engine]
        print(f"{engine:<20} {stats['total_time']:<15.4f} {stats['avg_time']:<15.4f} {stats['queries']:<10}")

    print("\n" + "=" * 80)
    print("PER-QUERY COMPARISON")
    print("=" * 80)
    print(f"{'Query':<30} {'DuckDB MV':<12} {'DuckDB Full':<12} {'MV Speedup':<15}")
    print("-" * 80)
    for item in comparison[:10]:
        qname = item["query"][:28]
        mv_t = item["duckdb_mv"]["seconds"]
        full_t = item["duckdb_full"]["seconds"]
        speedup = item["speedup"]["mv_vs_full"]
        mv_str = f"{mv_t:.4f}s" if mv_t else "N/A"
        full_str = f"{full_t:.4f}s" if full_t else "N/A"
        speedup_str = f"{speedup:.1f}x" if speedup else "N/A"
        print(f"{qname:<30} {mv_str:<12} {full_str:<12} {speedup_str:<15}")

    if len(comparison) > 10:
        print(f"... and {len(comparison) - 10} more queries")

    print("=" * 80)

    return report


def _write_markdown_report(report: dict, md_path: Path):
    md_lines = []
    md_lines.append("# Benchmark Comparison\n")
    s = report["summary"]
    md_lines.append("## Summary\n")
    md_lines.append("Engine | Total Time (s) | Avg Time (s) | Queries")
    md_lines.append("---|---:|---:|---:")
    for engine in ["duckdb_mv", "duckdb_full", "timescale"]:
        stats = s[engine]
        md_lines.append(f"{engine} | {stats['total_time']:.4f} | {stats['avg_time']:.4f} | {stats['queries']}")
    
    md_lines.append("\n## Per-query (first 20)\n")
    md_lines.append("Query | DuckDB MV (s) | DuckDB Full (s) | TimescaleDB (s) | MV Speedup vs Full")
    md_lines.append("---|---:|---:|---:|---:")
    for item in report["queries"][:20]:
        mv_t = item["duckdb_mv"]["seconds"]
        full_t = item["duckdb_full"]["seconds"]
        ts_t = item["timescale"]["seconds"]
        speed = item["speedup"]["mv_vs_full"]
        md_lines.append(
            f"{item['query']} | {mv_t if mv_t is not None else 'N/A'} | "
            f"{full_t if full_t is not None else 'N/A'} | "
            f"{ts_t if ts_t is not None else 'N/A'} | "
            f"{speed if speed is not None else 'N/A'}x"
        )
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(md_lines))


def main():
    parser = argparse.ArgumentParser(description="Benchmark DuckDB: MV vs Full Scan")
    parser.add_argument("--lake", required=True, help="Path to Parquet lake")
    parser.add_argument("--mvs", required=True, help="Path to DuckDB MVs")
    parser.add_argument("--queries", required=True, help="Directory with JSON queries")
    parser.add_argument("--out", required=True, help="Output directory for results")
    args = parser.parse_args()
    
    # Setup DuckDB connection
    print("[benchmark] Setting up DuckDB...")
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4;")
    con.execute("SET memory_limit='6GB';")
    
    # Run benchmarks
    print("[benchmark] Starting benchmarks...")
    
    # 1. DuckDB with MVs
    duckdb_mv_results = run_duckdb_mv_queries(args.lake, args.mvs, args.queries, con)
    
    # 2. DuckDB full scan
    duckdb_full_results = run_duckdb_full_scan_queries(args.lake, args.queries, con)
    
    # Generate comparison report
    report_path = Path(args.out) / "benchmark_comparison.json"
    report = generate_comparison_report(duckdb_mv_results, duckdb_full_results, report_path)
    
    # Also write a Markdown summary for quick viewing
    _write_markdown_report(report, Path(args.out) / "benchmark_comparison.md")
    
    con.close()


if __name__ == "__main__":
    main()
