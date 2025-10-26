#!/usr/bin/env python3
"""
Resource Pressure Testing System

Tests system behavior under controlled resource constraints to identify:
- Memory usage patterns and limits
- Performance degradation under pressure
- OOM prevention and graceful degradation
- Resource utilization efficiency
"""

import argparse
import json
import time
import statistics
import psutil
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional
import duckdb
import orjson
from dataclasses import dataclass

@dataclass
class ResourceSnapshot:
    timestamp: float
    memory_mb: float
    cpu_percent: float
    query_active: bool
    
@dataclass
class PressureTestResult:
    query_file: str
    table_used: str
    memory_limit: str
    execution_time_ms: float
    peak_memory_mb: float
    avg_memory_mb: float
    cpu_avg_percent: float
    success: bool
    error_message: Optional[str]
    rows_processed: int
    memory_efficiency_score: float

class ResourceMonitor:
    def __init__(self, interval: float = 0.1):
        self.interval = interval
        self.snapshots: List[ResourceSnapshot] = []
        self._stop_event = threading.Event()
        self._query_active = False
        
    def start_monitoring(self):
        """Start resource monitoring in background thread."""
        self._stop_event.clear()
        self.snapshots = []
        monitor_thread = threading.Thread(target=self._monitor_loop)
        monitor_thread.daemon = True
        monitor_thread.start()
        
    def set_query_active(self, active: bool):
        """Mark when query execution is active."""
        self._query_active = active
        
    def stop_monitoring(self):
        """Stop resource monitoring and return collected data."""
        self._stop_event.set()
        time.sleep(self.interval * 2)  # Allow final snapshots
        return self.snapshots.copy()
        
    def _monitor_loop(self):
        """Background monitoring loop."""
        process = psutil.Process()
        
        while not self._stop_event.is_set():
            try:
                # Get memory info in MB
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                
                # Get CPU percentage
                cpu_percent = process.cpu_percent()
                
                snapshot = ResourceSnapshot(
                    timestamp=time.time(),
                    memory_mb=memory_mb,
                    cpu_percent=cpu_percent,
                    query_active=self._query_active
                )
                
                self.snapshots.append(snapshot)
                
            except Exception:
                pass  # Ignore monitoring errors
                
            time.sleep(self.interval)

class ResourcePressureTester:
    def __init__(self, lake_path: str, mvs_path: str, threads: int = 4):
        self.lake_path = lake_path
        self.mvs_path = mvs_path  
        self.threads = threads
        self.results: List[PressureTestResult] = []
        
    def test_memory_limits(self, queries_dir: str, memory_limits: List[str]) -> Dict[str, Any]:
        """Test queries under different memory pressure levels."""
        query_files = sorted(Path(queries_dir).glob("*.json"))
        
        print(f"🔬 Starting resource pressure tests:")
        print(f"   📁 Queries: {len(query_files)}")
        print(f"   💾 Memory limits: {', '.join(memory_limits)}")
        print(f"   🧵 Threads: {self.threads}")
        print()
        
        for memory_limit in memory_limits:
            print(f"🧪 Testing with memory limit: {memory_limit}")
            
            for query_file in query_files:
                try:
                    result = self.run_pressure_test(query_file, memory_limit)
                    self.results.append(result)
                    
                    # Print immediate feedback
                    status = "✅" if result.success else "❌"
                    print(f"   {status} {query_file.name}: {result.execution_time_ms:.0f}ms, "
                          f"peak {result.peak_memory_mb:.0f}MB")
                    
                except Exception as e:
                    print(f"   💥 {query_file.name}: {str(e)}")
                    
                # Brief pause between tests
                time.sleep(2)
                
            print()
            
        return self.generate_report()
        
    def run_pressure_test(self, query_file: Path, memory_limit: str) -> PressureTestResult:
        """Run single query under memory pressure with monitoring."""
        # Load query
        query = orjson.loads(query_file.read_bytes())
        
        # Determine routing
        from planner import choose_plan
        table, proj = choose_plan(query)
        
        # Setup monitoring
        monitor = ResourceMonitor(interval=0.05)  # High frequency monitoring
        monitor.start_monitoring()
        
        # Setup connection with memory limit
        con = None
        success = False
        error_message = None
        rows_processed = 0
        execution_time_ms = 0
        
        try:
            con = duckdb.connect(database=":memory:")
            con.execute(f"PRAGMA threads={self.threads};")
            con.execute(f"SET memory_limit='{memory_limit}';")
            
            # Setup MV catalog and events view
            self._setup_query_environment(con, query, table)
            
            # Build and execute query
            sql = self._build_query_sql(query, table, con)
            
            # Execute with monitoring
            monitor.set_query_active(True)
            start_time = time.perf_counter()
            
            result_rows = con.execute(sql).fetchall()
            
            end_time = time.perf_counter()
            monitor.set_query_active(False)
            
            execution_time_ms = (end_time - start_time) * 1000
            rows_processed = len(result_rows)
            success = True
            
        except Exception as e:
            error_message = str(e)
            monitor.set_query_active(False)
            
        finally:
            if con:
                con.close()
                
        # Collect monitoring data
        snapshots = monitor.stop_monitoring()
        
        # Calculate resource metrics
        query_snapshots = [s for s in snapshots if s.query_active]
        if query_snapshots:
            peak_memory = max(s.memory_mb for s in query_snapshots)
            avg_memory = statistics.mean(s.memory_mb for s in query_snapshots)
            avg_cpu = statistics.mean(s.cpu_percent for s in query_snapshots)
        else:
            peak_memory = avg_memory = avg_cpu = 0
            
        # Calculate efficiency score (rows per MB)
        memory_limit_mb = self._parse_memory_limit(memory_limit)
        efficiency_score = rows_processed / memory_limit_mb if memory_limit_mb > 0 else 0
        
        return PressureTestResult(
            query_file=query_file.name,
            table_used=table,
            memory_limit=memory_limit,
            execution_time_ms=execution_time_ms,
            peak_memory_mb=peak_memory,
            avg_memory_mb=avg_memory,
            cpu_avg_percent=avg_cpu,
            success=success,
            error_message=error_message,
            rows_processed=rows_processed,
            memory_efficiency_score=efficiency_score
        )
        
    def _parse_memory_limit(self, limit: str) -> float:
        """Parse memory limit string to MB."""
        limit = limit.upper()
        if limit.endswith('GB'):
            return float(limit[:-2]) * 1024
        elif limit.endswith('MB'):
            return float(limit[:-2])
        else:
            return 0
            
    def _setup_query_environment(self, con: duckdb.DuckDBPyConnection, query: Dict, table: str):
        """Setup MV catalog and events view for query."""
        # Setup MV catalog
        mv_candidates = [
            "mv_day_minute_impr", "mv_day_country_publisher_impr", "mv_all_adv_type_counts",
            "mv_day_advertiser_id_wide", "mv_hour_advertiser_id_wide", "mv_week_advertiser_id_wide",
            "mv_day_publisher_id_wide", "mv_hour_publisher_id_wide", "mv_week_publisher_id_wide",
            "mv_day_country_wide", "mv_hour_country_wide", "mv_week_country_wide",
            "mv_day_type_wide", "mv_hour_type_wide", "mv_week_type_wide",
            "mv_day_user_id_wide", "mv_hour_user_id_wide", "mv_week_user_id_wide",
        ]
        
        # Setup events view if needed
        if table == "events_v":
            con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            
    def _build_query_sql(self, query: Dict, table: str, con: duckdb.DuckDBPyConnection) -> str:
        """Build SQL query for execution."""
        from runner import project_from_json, build_where, build_order_by
        
        # Get table source
        if table.startswith("mv_"):
            mv_path = Path(self.mvs_path) / table
            if mv_path.exists():
                if mv_path.is_dir():
                    src_table = f"read_parquet('{mv_path}/**/*.parquet')"
                else:
                    src_table = f"read_parquet('{mv_path}')"
            else:
                src_table = "events_v"
        else:
            src_table = "events_v"
            
        # Build SQL components
        select_sql = project_from_json(query, table)
        where_sql = build_where(query.get("where"), [])
        group_by = query.get("group_by", [])
        group_sql = f"GROUP BY {', '.join(group_by)}" if group_by else ""
        order_sql = build_order_by(query.get("order_by"), query.get("select"))
        limit_sql = f"LIMIT {query.get('limit')}" if query.get("limit") else ""
        
        return f"SELECT {select_sql} FROM {src_table} {where_sql} {group_sql} {order_sql} {limit_sql}".strip()
        
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive pressure test report."""
        if not self.results:
            return {"error": "No test results"}
            
        # Group results by memory limit
        limits = list(set(r.memory_limit for r in self.results))
        limits.sort(key=self._parse_memory_limit)
        
        # Calculate success rates by memory limit
        success_by_limit = {}
        performance_by_limit = {}
        
        for limit in limits:
            limit_results = [r for r in self.results if r.memory_limit == limit]
            success_count = sum(1 for r in limit_results if r.success)
            success_rate = success_count / len(limit_results) if limit_results else 0
            
            successful_results = [r for r in limit_results if r.success]
            if successful_results:
                avg_time = statistics.mean(r.execution_time_ms for r in successful_results)
                avg_peak_memory = statistics.mean(r.peak_memory_mb for r in successful_results)
                avg_efficiency = statistics.mean(r.memory_efficiency_score for r in successful_results)
            else:
                avg_time = avg_peak_memory = avg_efficiency = 0
                
            success_by_limit[limit] = success_rate
            performance_by_limit[limit] = {
                "success_rate": success_rate,
                "avg_execution_time_ms": avg_time,
                "avg_peak_memory_mb": avg_peak_memory,
                "avg_efficiency_score": avg_efficiency
            }
            
        # Identify memory pressure threshold
        pressure_threshold = None
        for limit in limits:
            if success_by_limit[limit] < 0.8:  # Less than 80% success rate
                pressure_threshold = limit
                break
                
        report = {
            "test_config": {
                "memory_limits_tested": limits,
                "queries_per_limit": len([r for r in self.results if r.memory_limit == limits[0]]) if limits else 0,
                "total_tests": len(self.results),
                "threads": self.threads,
                "timestamp": time.time()
            },
            "pressure_analysis": {
                "memory_pressure_threshold": pressure_threshold,
                "success_rates_by_limit": success_by_limit,
                "performance_by_limit": performance_by_limit
            },
            "detailed_results": [
                {
                    "query": r.query_file,
                    "memory_limit": r.memory_limit,
                    "table": r.table_used,
                    "success": r.success,
                    "execution_time_ms": round(r.execution_time_ms, 2),
                    "peak_memory_mb": round(r.peak_memory_mb, 1),
                    "efficiency_score": round(r.memory_efficiency_score, 2),
                    "error": r.error_message
                } for r in self.results
            ],
            "insights": self._generate_pressure_insights(performance_by_limit, pressure_threshold)
        }
        
        return report
        
    def _generate_pressure_insights(self, performance_by_limit: Dict, pressure_threshold: Optional[str]) -> List[str]:
        """Generate insights about resource pressure behavior."""
        insights = []
        
        if pressure_threshold:
            insights.append(f"Memory pressure threshold identified at {pressure_threshold} - queries start failing below this limit")
        else:
            insights.append("No clear memory pressure threshold found - system appears robust across tested limits")
            
        # Analyze performance degradation
        limits = sorted(performance_by_limit.keys(), key=self._parse_memory_limit, reverse=True)
        if len(limits) >= 2:
            high_limit_perf = performance_by_limit[limits[0]]
            low_limit_perf = performance_by_limit[limits[-1]]
            
            if low_limit_perf["avg_execution_time_ms"] > 0 and high_limit_perf["avg_execution_time_ms"] > 0:
                slowdown_factor = low_limit_perf["avg_execution_time_ms"] / high_limit_perf["avg_execution_time_ms"]
                if slowdown_factor > 2.0:
                    insights.append(f"Significant performance degradation: {slowdown_factor:.1f}x slower at low memory limits")
                elif slowdown_factor > 1.5:
                    insights.append(f"Moderate performance impact: {slowdown_factor:.1f}x slower under memory pressure")
                else:
                    insights.append(f"Minimal performance impact: {slowdown_factor:.1f}x slowdown shows good memory management")
                    
        # Memory efficiency analysis
        efficiency_scores = [r.memory_efficiency_score for r in self.results if r.success and r.memory_efficiency_score > 0]
        if efficiency_scores:
            avg_efficiency = statistics.mean(efficiency_scores)
            insights.append(f"Average memory efficiency: {avg_efficiency:.0f} rows processed per MB allocated")
            
        return insights
        
    def export_report(self, output_path: str) -> str:
        """Export pressure test report to JSON file."""
        report = self.generate_report()
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
            
        return output_path

def main():
    parser = argparse.ArgumentParser(description="Resource Pressure Testing")
    parser.add_argument("--lake", required=True, help="Path to Parquet lake")
    parser.add_argument("--mvs", required=True, help="Path to materialized views")
    parser.add_argument("--queries", required=True, help="Directory of JSON query files")
    parser.add_argument("--out", required=True, help="Output path for test report")
    parser.add_argument("--memory-limits", nargs="+", default=["6GB", "4GB", "2GB", "1GB"], 
                        help="Memory limits to test")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    args = parser.parse_args()
    
    # Create pressure tester
    tester = ResourcePressureTester(
        lake_path=args.lake,
        mvs_path=args.mvs,
        threads=args.threads
    )
    
    # Run pressure tests
    report = tester.test_memory_limits(args.queries, args.memory_limits)
    
    # Export results
    report_path = tester.export_report(args.out)
    
    print(f"🎯 Pressure Testing Complete!")
    print(f"   📊 Report: {report_path}")
    print(f"   🧪 Total tests: {report['test_config']['total_tests']}")
    
    if "pressure_analysis" in report:
        threshold = report["pressure_analysis"]["memory_pressure_threshold"]
        if threshold:
            print(f"   ⚠️  Pressure threshold: {threshold}")
        else:
            print(f"   ✅ No pressure threshold found - system robust")
            
    # Print insights
    if "insights" in report:
        print(f"\n💡 Key Insights:")
        for insight in report["insights"]:
            print(f"   • {insight}")

if __name__ == "__main__":
    main()