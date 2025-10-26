#!/usr/bin/env python3
"""
Cold vs Warm Cache Benchmarking System

Measures query performance differences between cold (first run) and warm (cached) executions
to quantify IO gains from parquet pruning, DuckDB caching, and OS page cache effects.
"""

import argparse
import json
import time
import statistics
from pathlib import Path
from typing import Dict, List, Tuple, Any
import duckdb
import orjson
from dataclasses import dataclass

@dataclass
class BenchmarkResult:
    query_file: str
    table_used: str
    cold_time_ms: float
    warm_times_ms: List[float]
    avg_warm_time_ms: float
    cache_speedup: float
    cold_rows: int
    warm_rows: int
    io_difference_estimated: bool

class CacheBenchmarker:
    def __init__(self, lake_path: str, mvs_path: str, threads: int = 4, memory: str = "6GB"):
        self.lake_path = lake_path
        self.mvs_path = mvs_path
        self.threads = threads
        self.memory = memory
        self.results: List[BenchmarkResult] = []
        
    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a fresh DuckDB connection with clean state."""
        con = duckdb.connect(database=":memory:")
        con.execute(f"PRAGMA threads={self.threads};")
        con.execute(f"SET memory_limit='{self.memory}';")
        con.execute("PRAGMA enable_object_cache=false;")  # Disable for cold runs
        return con
        
    def clear_os_cache(self):
        """Attempt to clear OS page cache (best effort)."""
        try:
            import subprocess
            # On macOS, this helps but requires sudo - skip for now
            # subprocess.run(['sudo', 'purge'], check=True)
            pass
        except Exception:
            pass
            
    def setup_mv_catalog(self, con: duckdb.DuckDBPyConnection) -> Dict[str, str]:
        """Setup MV catalog for query routing."""
        mv_candidates = [
            "mv_day_minute_impr",
            "mv_day_country_publisher_impr", 
            "mv_all_adv_type_counts",
            "mv_day_advertiser_id_wide", "mv_hour_advertiser_id_wide", "mv_week_advertiser_id_wide",
            "mv_day_publisher_id_wide", "mv_hour_publisher_id_wide", "mv_week_publisher_id_wide",
            "mv_day_country_wide", "mv_hour_country_wide", "mv_week_country_wide",
            "mv_day_type_wide", "mv_hour_type_wide", "mv_week_type_wide",
            "mv_day_user_id_wide", "mv_hour_user_id_wide", "mv_week_user_id_wide",
        ]
        
        mv_glob = {}
        for name in mv_candidates:
            p = Path(self.mvs_path) / name
            if p.exists():
                if p.is_dir():
                    pattern = f"{p}/**/*.parquet"
                else:
                    pattern = str(p)
                mv_glob[name] = pattern
                
        return mv_glob
        
    def setup_events_view(self, con: duckdb.DuckDBPyConnection, where_clause: List[Dict] = None):
        """Setup events_v with optional partition pruning."""
        if where_clause:
            # Extract date filters for partition pruning
            day_filters = [c for c in where_clause if c.get("col") == "day"]
            if day_filters:
                # Use partition pruning for targeted scans
                for df in day_filters:
                    if df.get("op") == "eq":
                        day_val = df["val"]
                        pattern = f"{self.lake_path}/events/day={day_val}/**/*.parquet"
                        con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet('{pattern}');")
                        return
                    elif df.get("op") == "between":
                        lo, hi = df["val"]
                        # For simplicity, use wildcard for date ranges in benchmarking
                        break
        
        # Fallback to full scan pattern
        con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
        
    def run_single_benchmark(self, query_file: Path, warm_runs: int = 3) -> BenchmarkResult:
        """Run cold + warm benchmark for a single query."""
        print(f"🧪 Benchmarking {query_file.name}")
        
        # Load query
        query = orjson.loads(query_file.read_bytes())
        
        # Determine routing (simplified)
        from planner import choose_plan
        table, proj = choose_plan(query)
        
        print(f"   📋 Routed to: {table}")
        
        # === COLD RUN ===
        print(f"   ❄️  Cold run...")
        self.clear_os_cache()
        con_cold = self.setup_connection()
        mv_glob = self.setup_mv_catalog(con_cold)
        
        if table == "events_v":
            self.setup_events_view(con_cold, query.get("where"))
        
        # Build SQL (simplified query building)
        cold_sql = self._build_query_sql(query, table, mv_glob)
        
        # Execute cold run with timing
        start = time.perf_counter()
        con_cold.execute("PRAGMA enable_profiling;")
        result_cold = con_cold.execute(cold_sql).fetchall()
        cold_time = (time.perf_counter() - start) * 1000
        
        # Get profiling info
        try:
            profile = con_cold.execute("PRAGMA profiling_output;").fetchone()
            cold_profile = str(profile[0]) if profile else ""
        except:
            cold_profile = ""
            
        con_cold.close()
        
        # === WARM RUNS ===
        print(f"   🔥 Warm runs ({warm_runs}x)...")
        con_warm = self.setup_connection()
        con_warm.execute("PRAGMA enable_object_cache=true;")  # Enable caching
        mv_glob = self.setup_mv_catalog(con_warm)
        
        if table == "events_v":
            self.setup_events_view(con_warm, query.get("where"))
            
        warm_times = []
        warm_sql = self._build_query_sql(query, table, mv_glob)
        
        for i in range(warm_runs):
            start = time.perf_counter()
            result_warm = con_warm.execute(warm_sql).fetchall()
            warm_time = (time.perf_counter() - start) * 1000
            warm_times.append(warm_time)
            
        con_warm.close()
        
        # Calculate metrics
        avg_warm = statistics.mean(warm_times)
        speedup = cold_time / avg_warm if avg_warm > 0 else 0
        
        result = BenchmarkResult(
            query_file=query_file.name,
            table_used=table,
            cold_time_ms=cold_time,
            warm_times_ms=warm_times,
            avg_warm_time_ms=avg_warm,
            cache_speedup=speedup,
            cold_rows=len(result_cold),
            warm_rows=len(result_warm),
            io_difference_estimated=True
        )
        
        print(f"   ⚡ Cold: {cold_time:.1f}ms, Warm: {avg_warm:.1f}ms, Speedup: {speedup:.1f}x")
        
        self.results.append(result)
        return result
        
    def _build_query_sql(self, query: Dict, table: str, mv_glob: Dict[str, str]) -> str:
        """Build SQL query (simplified version)."""
        from runner import project_from_json, build_where, build_order_by
        
        # Get table source
        if table in mv_glob:
            src_table = f"read_parquet('{mv_glob[table]}')"
        else:
            src_table = "events_v"
            
        # Build components
        select_sql = project_from_json(query, table)
        where_sql = build_where(query.get("where"), [])  # No filter pruning for benchmarking
        group_by = query.get("group_by", [])
        group_sql = f"GROUP BY {', '.join(group_by)}" if group_by else ""
        order_sql = build_order_by(query.get("order_by"), query.get("select"))
        limit_sql = f"LIMIT {query.get('limit')}" if query.get("limit") else ""
        
        return f"SELECT {select_sql} FROM {src_table} {where_sql} {group_sql} {order_sql} {limit_sql}".strip()
        
    def run_benchmark_suite(self, queries_dir: str, warm_runs: int = 3) -> Dict[str, Any]:
        """Run benchmark suite on all queries in directory."""
        query_files = sorted(Path(queries_dir).glob("*.json"))
        
        print(f"🚀 Starting cache benchmark suite:")
        print(f"   📁 Queries: {len(query_files)} from {queries_dir}")
        print(f"   🔄 Warm runs per query: {warm_runs}")
        print(f"   💾 Memory limit: {self.memory}")
        print(f"   🧵 Threads: {self.threads}")
        print()
        
        for query_file in query_files:
            try:
                self.run_single_benchmark(query_file, warm_runs)
                time.sleep(1)  # Brief pause between queries
            except Exception as e:
                print(f"   ❌ Failed: {e}")
                continue
                
        return self.generate_report()
        
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive benchmark report."""
        if not self.results:
            return {"error": "No benchmark results"}
            
        # Calculate aggregate statistics
        cold_times = [r.cold_time_ms for r in self.results]
        warm_times = [r.avg_warm_time_ms for r in self.results]
        speedups = [r.cache_speedup for r in self.results]
        
        # Separate MV vs fallback results
        mv_results = [r for r in self.results if r.table_used != "events_v"]
        fallback_results = [r for r in self.results if r.table_used == "events_v"]
        
        report = {
            "benchmark_config": {
                "memory_limit": self.memory,
                "threads": self.threads,
                "queries_tested": len(self.results),
                "timestamp": time.time()
            },
            "aggregate_metrics": {
                "avg_cold_time_ms": statistics.mean(cold_times),
                "avg_warm_time_ms": statistics.mean(warm_times),
                "avg_cache_speedup": statistics.mean(speedups),
                "median_cache_speedup": statistics.median(speedups),
                "max_cache_speedup": max(speedups),
                "cold_time_range_ms": [min(cold_times), max(cold_times)],
                "warm_time_range_ms": [min(warm_times), max(warm_times)]
            },
            "mv_vs_fallback": {
                "mv_queries": len(mv_results),
                "fallback_queries": len(fallback_results),
                "mv_avg_cold_ms": statistics.mean([r.cold_time_ms for r in mv_results]) if mv_results else 0,
                "mv_avg_warm_ms": statistics.mean([r.avg_warm_time_ms for r in mv_results]) if mv_results else 0,
                "fallback_avg_cold_ms": statistics.mean([r.cold_time_ms for r in fallback_results]) if fallback_results else 0,
                "fallback_avg_warm_ms": statistics.mean([r.avg_warm_time_ms for r in fallback_results]) if fallback_results else 0
            },
            "detailed_results": [
                {
                    "query": r.query_file,
                    "table": r.table_used,
                    "cold_ms": round(r.cold_time_ms, 2),
                    "warm_ms": round(r.avg_warm_time_ms, 2),
                    "speedup": round(r.cache_speedup, 1),
                    "rows": r.cold_rows
                } for r in self.results
            ],
            "insights": self._generate_insights()
        }
        
        return report
        
    def _generate_insights(self) -> List[str]:
        """Generate performance insights from benchmark results."""
        insights = []
        
        if not self.results:
            return ["No results to analyze"]
            
        # Cache effectiveness analysis
        speedups = [r.cache_speedup for r in self.results]
        avg_speedup = statistics.mean(speedups)
        
        if avg_speedup > 2.0:
            insights.append(f"Excellent cache performance: {avg_speedup:.1f}x average speedup indicates strong IO benefits")
        elif avg_speedup > 1.5:
            insights.append(f"Good cache performance: {avg_speedup:.1f}x average speedup shows moderate IO gains")
        else:
            insights.append(f"Limited cache benefits: {avg_speedup:.1f}x speedup suggests CPU-bound or small data queries")
            
        # MV vs fallback analysis
        mv_results = [r for r in self.results if r.table_used != "events_v"]
        fallback_results = [r for r in self.results if r.table_used == "events_v"]
        
        if mv_results and fallback_results:
            mv_avg = statistics.mean([r.avg_warm_time_ms for r in mv_results])
            fallback_avg = statistics.mean([r.avg_warm_time_ms for r in fallback_results])
            mv_advantage = fallback_avg / mv_avg if mv_avg > 0 else 0
            
            insights.append(f"MV advantage: {mv_advantage:.1f}x faster than fallback queries on average")
            
        # Identify best cache candidates
        best_cache_queries = sorted(self.results, key=lambda r: r.cache_speedup, reverse=True)[:3]
        if best_cache_queries:
            best_names = [r.query_file for r in best_cache_queries[:3]]
            insights.append(f"Best cache candidates: {', '.join(best_names)}")
            
        return insights
        
    def export_report(self, output_path: str) -> str:
        """Export benchmark report to JSON file."""
        report = self.generate_report()
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
            
        return output_path

def main():
    parser = argparse.ArgumentParser(description="Cold vs Warm Cache Benchmarking")
    parser.add_argument("--lake", required=True, help="Path to Parquet lake")
    parser.add_argument("--mvs", required=True, help="Path to materialized views")
    parser.add_argument("--queries", required=True, help="Directory of JSON query files")
    parser.add_argument("--out", required=True, help="Output path for benchmark report")
    parser.add_argument("--warm-runs", type=int, default=3, help="Number of warm runs per query")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    parser.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    args = parser.parse_args()
    
    # Create benchmarker
    benchmarker = CacheBenchmarker(
        lake_path=args.lake,
        mvs_path=args.mvs,
        threads=args.threads,
        memory=args.mem
    )
    
    # Run benchmark suite
    report = benchmarker.run_benchmark_suite(args.queries, args.warm_runs)
    
    # Export results
    report_path = benchmarker.export_report(args.out)
    
    print(f"\n🎯 Benchmark Complete!")
    print(f"   📊 Report: {report_path}")
    print(f"   ⚡ Average cache speedup: {report['aggregate_metrics']['avg_cache_speedup']:.1f}x")
    print(f"   🏆 Max speedup: {report['aggregate_metrics']['max_cache_speedup']:.1f}x")
    
    # Print top insights
    print(f"\n💡 Key Insights:")
    for insight in report['insights']:
        print(f"   • {insight}")

if __name__ == "__main__":
    main()