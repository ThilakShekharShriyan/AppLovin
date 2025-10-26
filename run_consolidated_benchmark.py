#!/usr/bin/env python3
"""
Consolidated Query Benchmark Runner

Runs all queries from the consolidated test suite and provides detailed timing analysis.
"""

import json
import time
import sys
import os
from pathlib import Path
from typing import List, Dict, Any
import statistics

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

from safe_batch_runner import SafeBatchRunner

class ConsolidatedBenchmark:
    """Runs comprehensive timing benchmarks on consolidated query suite."""
    
    def __init__(self, memory: str = "12GB", threads: int = 8):
        self.memory = memory
        self.threads = threads
        self.lake_path = "data/lake"
        self.mvs_path = "data/mvs_rebuilt"
        self.output_path = "results/consolidated_benchmark"
        
        # Ensure output directory exists
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        
        print(f"🚀 Consolidated Query Benchmark Runner")
        print(f"📊 Configuration: {memory} memory, {threads} threads")
        print(f"🏗️  Lake: {self.lake_path}")
        print(f"📚 MVs: {self.mvs_path}")
        print()
        
    def load_query_suite(self) -> List[Dict[str, Any]]:
        """Load the consolidated query test suite."""
        suite_path = Path("queries/consolidated_test_suite.json")
        
        if not suite_path.exists():
            raise FileNotFoundError(f"Consolidated test suite not found at {suite_path}")
            
        with open(suite_path, 'r') as f:
            queries = json.load(f)
            
        print(f"📋 Loaded {len(queries)} queries from consolidated test suite")
        return queries
        
    def execute_query_with_timing(self, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single query with detailed timing."""
        
        name = query_spec["name"]
        category = query_spec["category"]
        query = query_spec["query"]
        
        print(f"   📋 {name} ({category})")
        
        # Initialize runner
        runner = SafeBatchRunner(
            lake_path=self.lake_path,
            mvs_path=self.mvs_path,
            output_path=self.output_path,
            memory=self.memory,
            threads=self.threads
        )
        
        # Time the query execution
        start_time = time.perf_counter()
        
        try:
            # Execute query directly
            exec_start = time.perf_counter()
            results = runner.execute_single_query(query, f"{name}.csv")
            exec_time = (time.perf_counter() - exec_start) * 1000
            
            total_time = (time.perf_counter() - start_time) * 1000
            
            # Extract execution info
            table_used = results.get('table', 'unknown')
            row_count = len(results.get('results', []))
            
            result = {
                "name": name,
                "category": category,
                "success": True,
                "total_time_ms": round(total_time, 2),
                "execution_time_ms": round(exec_time, 2),
                "table_used": table_used,
                "row_count": row_count,
                "error": None
            }
            
            print(f"      ⚡ {total_time:.1f}ms total ({exec_time:.1f}ms exec)")
            print(f"      📊 {row_count} rows from {table_used}")
            
        except Exception as e:
            total_time = (time.perf_counter() - start_time) * 1000
            
            result = {
                "name": name,
                "category": category,
                "success": False,
                "total_time_ms": round(total_time, 2),
                "execution_time_ms": 0,
                "table_used": None,
                "row_count": 0,
                "error": str(e)
            }
            
            print(f"      ❌ Failed: {str(e)}")
            
        return result
        
    def run_benchmark(self) -> Dict[str, Any]:
        """Run the complete consolidated benchmark."""
        
        print("🔥 Starting Consolidated Query Benchmark")
        print("=" * 60)
        
        # Load query suite
        query_suite = self.load_query_suite()
        
        # Execute all queries
        results = []
        categories = {}
        
        benchmark_start = time.perf_counter()
        
        for query_spec in query_suite:
            result = self.execute_query_with_timing(query_spec)
            results.append(result)
            
            # Track by category
            category = result["category"]
            if category not in categories:
                categories[category] = []
            categories[category].append(result)
            
        total_benchmark_time = (time.perf_counter() - benchmark_start)
        
        # Generate comprehensive analysis
        analysis = self.analyze_results(results, categories, total_benchmark_time)
        
        # Save results
        self.save_results(analysis)
        
        # Print summary
        self.print_summary(analysis)
        
        return analysis
        
    def analyze_results(self, results: List[Dict], categories: Dict, total_time: float) -> Dict[str, Any]:
        """Analyze benchmark results."""
        
        successful_results = [r for r in results if r["success"]]
        failed_results = [r for r in results if not r["success"]]
        
        if not successful_results:
            return {
                "error": "No successful queries",
                "total_queries": len(results),
                "failed_queries": len(failed_results)
            }
            
        # Overall statistics
        exec_times = [r["execution_time_ms"] for r in successful_results]
        total_times = [r["total_time_ms"] for r in successful_results]
        row_counts = [r["row_count"] for r in successful_results]
        
        # Table usage analysis
        table_usage = {}
        for result in successful_results:
            table = result["table_used"]
            if table not in table_usage:
                table_usage[table] = []
            table_usage[table].append(result)
            
        # Category analysis
        category_stats = {}
        for category, cat_results in categories.items():
            success_results = [r for r in cat_results if r["success"]]
            if success_results:
                cat_exec_times = [r["execution_time_ms"] for r in success_results]
                category_stats[category] = {
                    "query_count": len(cat_results),
                    "successful": len(success_results),
                    "failed": len(cat_results) - len(success_results),
                    "avg_exec_time_ms": round(statistics.mean(cat_exec_times), 2),
                    "min_exec_time_ms": round(min(cat_exec_times), 2),
                    "max_exec_time_ms": round(max(cat_exec_times), 2),
                    "total_exec_time_ms": round(sum(cat_exec_times), 2)
                }
        
        return {
            "benchmark_summary": {
                "total_queries": len(results),
                "successful_queries": len(successful_results),
                "failed_queries": len(failed_results),
                "success_rate": round(len(successful_results) / len(results) * 100, 1),
                "total_benchmark_time_sec": round(total_time, 2),
                "total_execution_time_ms": round(sum(exec_times), 2),
                "queries_per_second": round(len(successful_results) / total_time, 2)
            },
            "performance_statistics": {
                "avg_execution_time_ms": round(statistics.mean(exec_times), 2),
                "median_execution_time_ms": round(statistics.median(exec_times), 2),
                "min_execution_time_ms": round(min(exec_times), 2),
                "max_execution_time_ms": round(max(exec_times), 2),
                "p95_execution_time_ms": round(statistics.quantiles(exec_times, n=20)[18], 2) if len(exec_times) > 20 else round(max(exec_times), 2),
                "total_rows_returned": sum(row_counts)
            },
            "table_usage_analysis": {
                table: {
                    "query_count": len(queries),
                    "avg_time_ms": round(statistics.mean([q["execution_time_ms"] for q in queries]), 2),
                    "total_rows": sum([q["row_count"] for q in queries])
                }
                for table, queries in table_usage.items()
            },
            "category_analysis": category_stats,
            "detailed_results": results,
            "configuration": {
                "memory": self.memory,
                "threads": self.threads,
                "lake_path": self.lake_path,
                "mvs_path": self.mvs_path
            }
        }
        
    def save_results(self, analysis: Dict[str, Any]) -> None:
        """Save benchmark results to file."""
        
        output_file = Path("reports/consolidated_benchmark_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(analysis, f, indent=2)
            
        print(f"💾 Results saved to: {output_file}")
        
    def print_summary(self, analysis: Dict[str, Any]) -> None:
        """Print benchmark summary."""
        
        if "error" in analysis:
            print(f"❌ Benchmark failed: {analysis['error']}")
            return
            
        summary = analysis["benchmark_summary"]
        perf = analysis["performance_statistics"]
        
        print()
        print("📊 Consolidated Benchmark Results")
        print("=" * 50)
        print(f"Total Queries: {summary['total_queries']}")
        print(f"Successful: {summary['successful_queries']} ({summary['success_rate']}%)")
        print(f"Failed: {summary['failed_queries']}")
        print(f"Benchmark Time: {summary['total_benchmark_time_sec']:.1f}s")
        print(f"Throughput: {summary['queries_per_second']:.1f} queries/sec")
        print()
        
        print("⚡ Performance Statistics:")
        print(f"  Average: {perf['avg_execution_time_ms']:.1f}ms")
        print(f"  Median: {perf['median_execution_time_ms']:.1f}ms")
        print(f"  Min: {perf['min_execution_time_ms']:.1f}ms")
        print(f"  Max: {perf['max_execution_time_ms']:.1f}ms")
        print(f"  P95: {perf['p95_execution_time_ms']:.1f}ms")
        print(f"  Total Rows: {perf['total_rows_returned']:,}")
        print()
        
        print("🎯 Table Usage:")
        table_usage = analysis["table_usage_analysis"]
        for table, stats in sorted(table_usage.items(), key=lambda x: x[1]['query_count'], reverse=True):
            percentage = (stats['query_count'] / summary['successful_queries']) * 100
            print(f"  {table}: {stats['query_count']} queries ({percentage:.1f}%) - {stats['avg_time_ms']:.1f}ms avg")
        print()
        
        print("📋 Category Performance:")
        categories = analysis["category_analysis"]
        for category, stats in sorted(categories.items(), key=lambda x: x[1]['avg_exec_time_ms']):
            print(f"  {category}: {stats['avg_exec_time_ms']:.1f}ms avg ({stats['successful']}/{stats['query_count']} queries)")
        
        print()
        print("🏆 Consolidated Benchmark Complete!")

def main():
    """Main execution function."""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Consolidated Query Benchmark Runner")
    parser.add_argument("--memory", default="12GB", help="Memory limit (default: 12GB)")
    parser.add_argument("--threads", type=int, default=8, help="Thread count (default: 8)")
    
    args = parser.parse_args()
    
    try:
        benchmark = ConsolidatedBenchmark(memory=args.memory, threads=args.threads)
        benchmark.run_benchmark()
        
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()