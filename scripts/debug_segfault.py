#!/usr/bin/env python3
"""
Segfault Diagnosis Toolkit

Helps identify root causes of concurrent indexing segfaults through:
1. Controlled test scenarios to isolate the issue
2. Safety validation checks
3. Schema drift detection
4. Connection safety verification
"""

import os
import sys
import time
import json
import uuid
import threading
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import duckdb

class SegfaultDiagnoser:
    """Systematic approach to diagnosing concurrent indexing segfaults."""
    
    def __init__(self, lake_path: str, mvs_path: str, output_path: str):
        self.lake_path = Path(lake_path)
        self.mvs_path = Path(mvs_path) 
        self.output_path = Path(output_path)
        self.results = []
        
    def test_connection_safety(self) -> Dict[str, Any]:
        """Test if connection sharing causes issues."""
        print("🔍 Testing connection safety...")
        
        def worker_shared_connection(shared_con, worker_id, results):
            """Worker using shared connection - UNSAFE."""
            try:
                for i in range(5):
                    result = shared_con.execute("SELECT 1 as test").fetchone()
                    time.sleep(0.01)  # Small delay to increase race chance
                results[worker_id] = "SUCCESS"
            except Exception as e:
                results[worker_id] = f"ERROR: {str(e)}"
                
        def worker_per_thread_connection(worker_id, results):
            """Worker with per-thread connection - SAFE."""
            try:
                con = duckdb.connect(":memory:")
                con.execute("PRAGMA threads=1;")
                for i in range(5):
                    result = con.execute("SELECT 1 as test").fetchone()
                    time.sleep(0.01)
                con.close()
                results[worker_id] = "SUCCESS"
            except Exception as e:
                results[worker_id] = f"ERROR: {str(e)}"
        
        # Test 1: Shared connection (unsafe)
        shared_con = duckdb.connect(":memory:")
        shared_results = {}
        
        threads = []
        for i in range(4):
            t = threading.Thread(target=worker_shared_connection, 
                               args=(shared_con, i, shared_results))
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()
            
        shared_con.close()
        
        # Test 2: Per-thread connections (safe)
        per_thread_results = {}
        
        threads = []
        for i in range(4):
            t = threading.Thread(target=worker_per_thread_connection, 
                               args=(i, per_thread_results))
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()
            
        shared_errors = len([r for r in shared_results.values() if r.startswith("ERROR")])
        per_thread_errors = len([r for r in per_thread_results.values() if r.startswith("ERROR")])
        
        return {
            "test": "connection_safety",
            "shared_connection_errors": shared_errors,
            "per_thread_connection_errors": per_thread_errors,
            "diagnosis": "SHARED_CONNECTION_ISSUE" if shared_errors > per_thread_errors else "CONNECTION_SAFE",
            "recommendation": "Use per-thread connections" if shared_errors > per_thread_errors else "Connection pattern is safe"
        }
        
    def test_concurrent_file_access(self) -> Dict[str, Any]:
        """Test concurrent read/write to same files."""
        print("🔍 Testing concurrent file access...")
        
        # Create test data
        test_dir = self.output_path / "test_concurrent"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        test_file = test_dir / "test.parquet"
        
        def create_test_data():
            """Create initial test file."""
            con = duckdb.connect(":memory:")
            con.execute(f"""
                COPY (
                    SELECT 
                        range as id, 
                        'test_' || range as name,
                        random() as value
                    FROM range(1000)
                ) TO '{test_file}' (FORMAT PARQUET)
            """)
            con.close()
            
        def concurrent_reader(reader_id, results):
            """Concurrent reader."""
            try:
                con = duckdb.connect(":memory:")
                for i in range(10):
                    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{test_file}')").fetchone()[0]
                    time.sleep(0.005)  # Small delay
                con.close()
                results[f"reader_{reader_id}"] = "SUCCESS"
            except Exception as e:
                results[f"reader_{reader_id}"] = f"ERROR: {str(e)}"
                
        def concurrent_writer(writer_id, results):
            """Concurrent writer to same directory."""
            try:
                con = duckdb.connect(":memory:")
                temp_file = test_dir / f"temp_writer_{writer_id}.parquet"
                
                con.execute(f"""
                    COPY (
                        SELECT 
                            range + {writer_id * 1000} as id,
                            'writer_{writer_id}_' || range as name,
                            random() as value
                        FROM range(100)
                    ) TO '{temp_file}' (FORMAT PARQUET)
                """)
                con.close()
                results[f"writer_{writer_id}"] = "SUCCESS"
            except Exception as e:
                results[f"writer_{writer_id}"] = f"ERROR: {str(e)}"
        
        # Create initial data
        create_test_data()
        
        # Test concurrent access
        results = {}
        threads = []
        
        # Start readers and writers concurrently
        for i in range(3):
            reader_t = threading.Thread(target=concurrent_reader, args=(i, results))
            writer_t = threading.Thread(target=concurrent_writer, args=(i, results))
            threads.extend([reader_t, writer_t])
            reader_t.start()
            writer_t.start()
            
        for t in threads:
            t.join()
            
        reader_errors = len([r for k, r in results.items() if k.startswith("reader") and r.startswith("ERROR")])
        writer_errors = len([r for k, r in results.items() if k.startswith("writer") and r.startswith("ERROR")])
        
        # Cleanup
        for f in test_dir.glob("*.parquet"):
            f.unlink(missing_ok=True)
            
        return {
            "test": "concurrent_file_access",
            "reader_errors": reader_errors,
            "writer_errors": writer_errors,
            "total_operations": len(results),
            "diagnosis": "FILE_ACCESS_RACE" if (reader_errors > 0 or writer_errors > 0) else "FILE_ACCESS_SAFE",
            "recommendation": "Use staging/ready pattern for atomic writes" if reader_errors > 0 else "File access pattern is safe"
        }
        
    def test_schema_consistency(self) -> Dict[str, Any]:
        """Test for schema drift issues."""
        print("🔍 Testing schema consistency...")
        
        test_dir = self.output_path / "test_schema" 
        test_dir.mkdir(parents=True, exist_ok=True)
        
        def create_schema_a(results):
            """Create file with schema A."""
            try:
                con = duckdb.connect(":memory:")
                file_path = test_dir / "schema_a.parquet"
                
                con.execute(f"""
                    COPY (
                        SELECT 
                            range as id,
                            range::DOUBLE as bid_price,
                            'type_a' as event_type
                        FROM range(100)
                    ) TO '{file_path}' (FORMAT PARQUET)
                """)
                con.close()
                results["schema_a"] = "SUCCESS"
            except Exception as e:
                results["schema_a"] = f"ERROR: {str(e)}"
                
        def create_schema_b(results):
            """Create file with schema B (conflicting types)."""
            try:
                con = duckdb.connect(":memory:")
                file_path = test_dir / "schema_b.parquet"
                
                con.execute(f"""
                    COPY (
                        SELECT 
                            range as id,
                            range::INTEGER as bid_price,  -- Different type!
                            'type_b' as event_type
                        FROM range(100)
                    ) TO '{file_path}' (FORMAT PARQUET)
                """)
                con.close()
                results["schema_b"] = "SUCCESS"
            except Exception as e:
                results["schema_b"] = f"ERROR: {str(e)}"
                
        def read_mixed_schemas(results):
            """Try to read files with mixed schemas."""
            try:
                con = duckdb.connect(":memory:")
                # This should fail or produce inconsistent results
                count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{test_dir}/*.parquet')").fetchone()[0]
                con.close()
                results["mixed_read"] = f"SUCCESS: {count} rows"
            except Exception as e:
                results["mixed_read"] = f"ERROR: {str(e)}"
        
        # Test concurrent schema creation
        results = {}
        threads = []
        
        schema_a_t = threading.Thread(target=create_schema_a, args=(results,))
        schema_b_t = threading.Thread(target=create_schema_b, args=(results,))
        
        schema_a_t.start()
        schema_b_t.start() 
        schema_a_t.join()
        schema_b_t.join()
        
        # Try to read mixed schemas
        read_mixed_schemas(results)
        
        # Cleanup
        for f in test_dir.glob("*.parquet"):
            f.unlink(missing_ok=True)
            
        schema_errors = len([r for r in results.values() if r.startswith("ERROR")])
        
        return {
            "test": "schema_consistency",
            "creation_results": {k: v for k, v in results.items() if k.startswith("schema")},
            "mixed_read_result": results.get("mixed_read", "NOT_TESTED"),
            "schema_errors": schema_errors,
            "diagnosis": "SCHEMA_DRIFT_ISSUE" if schema_errors > 0 else "SCHEMA_CONSISTENT",
            "recommendation": "Implement schema registry and validation" if schema_errors > 0 else "Schema handling is consistent"
        }
        
    def test_memory_pressure(self) -> Dict[str, Any]:
        """Test behavior under memory pressure."""
        print("🔍 Testing memory pressure scenarios...")
        
        def memory_intensive_query(query_id, results):
            """Run memory-intensive operation."""
            try:
                con = duckdb.connect(":memory:")
                con.execute("SET memory_limit='512MB';")  # Low limit
                
                # Generate large dataset in memory
                count = con.execute("""
                    WITH large_data AS (
                        SELECT 
                            range as id,
                            'data_' || range as text_col,
                            random() as val1,
                            random() as val2,
                            random() as val3
                        FROM range(1000000)  -- 1M rows
                    )
                    SELECT COUNT(*) FROM large_data
                    WHERE val1 > 0.5
                """).fetchone()[0]
                
                con.close()
                results[query_id] = f"SUCCESS: {count}"
            except Exception as e:
                results[query_id] = f"ERROR: {str(e)}"
        
        # Run multiple memory-intensive operations concurrently
        results = {}
        threads = []
        
        for i in range(3):  # 3 concurrent memory-intensive operations
            t = threading.Thread(target=memory_intensive_query, args=(f"mem_query_{i}", results))
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()
            
        memory_errors = len([r for r in results.values() if r.startswith("ERROR")])
        oom_errors = len([r for r in results.values() if "memory" in r.lower()])
        
        return {
            "test": "memory_pressure",
            "concurrent_operations": len(results),
            "memory_errors": memory_errors,
            "oom_related_errors": oom_errors,
            "results": results,
            "diagnosis": "MEMORY_PRESSURE_ISSUE" if memory_errors > 0 else "MEMORY_HANDLING_STABLE",
            "recommendation": "Implement memory guards and limits" if memory_errors > 0 else "Memory handling appears stable"
        }
        
    def test_serialized_execution(self) -> Dict[str, Any]:
        """Test if serialized execution eliminates issues."""
        print("🔍 Testing serialized vs concurrent execution...")
        
        def run_test_operations(concurrent: bool) -> Dict[str, Any]:
            """Run operations either concurrently or serially."""
            test_dir = self.output_path / f"test_{'concurrent' if concurrent else 'serial'}"
            test_dir.mkdir(parents=True, exist_ok=True)
            
            def operation(op_id, results):
                try:
                    con = duckdb.connect(":memory:")
                    con.execute("PRAGMA threads=1;")
                    
                    # Create test data
                    temp_file = test_dir / f"op_{op_id}.parquet"
                    con.execute(f"""
                        COPY (
                            SELECT 
                                range + {op_id * 1000} as id,
                                'op_{op_id}_' || range as name,
                                random() as value
                            FROM range(1000)
                        ) TO '{temp_file}' (FORMAT PARQUET)
                    """)
                    
                    # Read it back
                    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{temp_file}')").fetchone()[0]
                    con.close()
                    
                    results[f"op_{op_id}"] = f"SUCCESS: {count}"
                except Exception as e:
                    results[f"op_{op_id}"] = f"ERROR: {str(e)}"
                    
            results = {}
            operations = 4
            
            if concurrent:
                # Run concurrently
                threads = []
                for i in range(operations):
                    t = threading.Thread(target=operation, args=(i, results))
                    threads.append(t)
                    t.start()
                    
                for t in threads:
                    t.join()
            else:
                # Run serially
                for i in range(operations):
                    operation(i, results)
                    
            # Cleanup
            for f in test_dir.glob("*.parquet"):
                f.unlink(missing_ok=True)
                
            errors = len([r for r in results.values() if r.startswith("ERROR")])
            return {
                "mode": "concurrent" if concurrent else "serial",
                "operations": operations,
                "errors": errors,
                "results": results
            }
        
        concurrent_result = run_test_operations(True)
        serial_result = run_test_operations(False)
        
        diagnosis = "CONCURRENCY_ISSUE" if (concurrent_result["errors"] > serial_result["errors"]) else "CONCURRENCY_SAFE"
        
        return {
            "test": "serialized_execution",
            "concurrent_test": concurrent_result,
            "serial_test": serial_result,
            "diagnosis": diagnosis,
            "recommendation": "Serialize critical operations" if diagnosis == "CONCURRENCY_ISSUE" else "Concurrency appears safe"
        }
        
    def run_comprehensive_diagnosis(self) -> Dict[str, Any]:
        """Run all diagnostic tests."""
        print("🔧 Starting Comprehensive Segfault Diagnosis")
        print(f"   📁 Lake: {self.lake_path}")
        print(f"   📊 MVs: {self.mvs_path}")
        print(f"   📁 Output: {self.output_path}")
        print()
        
        # Ensure output directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Run all tests
        tests = [
            self.test_connection_safety,
            self.test_concurrent_file_access,
            self.test_schema_consistency,
            self.test_memory_pressure,
            self.test_serialized_execution
        ]
        
        results = []
        for test_func in tests:
            try:
                result = test_func()
                results.append(result)
                
                # Print immediate result
                status = "✅" if not result.get("diagnosis", "").endswith("_ISSUE") else "❌"
                print(f"   {status} {result['test']}: {result.get('diagnosis', 'Unknown')}")
                
            except Exception as e:
                error_result = {
                    "test": test_func.__name__,
                    "diagnosis": "TEST_FAILED",
                    "error": str(e),
                    "recommendation": "Fix test execution environment"
                }
                results.append(error_result)
                print(f"   ❌ {test_func.__name__}: TEST_FAILED - {str(e)}")
                
        return self.generate_diagnosis_report(results)
        
    def generate_diagnosis_report(self, test_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate comprehensive diagnosis report."""
        issues_found = []
        recommendations = []
        
        for result in test_results:
            if result.get("diagnosis", "").endswith("_ISSUE"):
                issues_found.append({
                    "test": result["test"],
                    "issue": result["diagnosis"],
                    "recommendation": result.get("recommendation", "No recommendation")
                })
                
        # Overall diagnosis
        if not issues_found:
            overall_diagnosis = "NO_ISSUES_DETECTED"
            overall_recommendation = "System appears stable for concurrent operations"
        elif len(issues_found) == 1:
            overall_diagnosis = f"SINGLE_ISSUE: {issues_found[0]['issue']}"
            overall_recommendation = issues_found[0]["recommendation"]
        else:
            overall_diagnosis = f"MULTIPLE_ISSUES: {len(issues_found)} problems detected"
            overall_recommendation = "Address all identified issues systematically"
            
        report = {
            "diagnosis_summary": {
                "timestamp": time.time(),
                "overall_diagnosis": overall_diagnosis,
                "issues_found": len(issues_found),
                "tests_run": len(test_results),
                "overall_recommendation": overall_recommendation
            },
            "detailed_results": test_results,
            "critical_issues": issues_found,
            "hardening_checklist": [
                "✅ Use per-thread DuckDB connections",
                "✅ Implement staging/ready directory pattern", 
                "✅ Add schema registry for consistency",
                "✅ Set memory limits and guards",
                "✅ Use atomic file operations (temp + rename)",
                "✅ Serialize write operations per MV",
                "✅ Add comprehensive error handling",
                "✅ Enable RUST_BACKTRACE=full for debugging"
            ],
            "emergency_fixes": [
                "Set APP_INDEXING_THREADS=1 to serialize operations",
                "Reduce memory_limit to prevent OOM crashes",
                "Use temp file patterns: write to .tmp, rename atomically",
                "Add try/catch around all DuckDB operations"
            ]
        }
        
        return report
        
    def export_report(self, report: Dict[str, Any]) -> str:
        """Export diagnosis report."""
        report_path = self.output_path / "segfault_diagnosis.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        return str(report_path)

def main():
    """CLI for segfault diagnosis."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Segfault Diagnosis Toolkit")
    parser.add_argument("--lake", required=True, help="Path to Parquet lake")
    parser.add_argument("--mvs", required=True, help="Path to materialized views")
    parser.add_argument("--out", required=True, help="Output directory for test results")
    parser.add_argument("--enable-debug", action="store_true", help="Enable debug mode (RUST_BACKTRACE=full)")
    args = parser.parse_args()
    
    # Enable debug mode if requested
    if args.enable_debug:
        os.environ["RUST_BACKTRACE"] = "full"
        print("🐛 Debug mode enabled (RUST_BACKTRACE=full)")
        
    # Run diagnosis
    diagnoser = SegfaultDiagnoser(
        lake_path=args.lake,
        mvs_path=args.mvs,
        output_path=args.out
    )
    
    report = diagnoser.run_comprehensive_diagnosis()
    report_path = diagnoser.export_report(report)
    
    # Print summary
    summary = report["diagnosis_summary"]
    print(f"\n🎯 Segfault Diagnosis Complete!")
    print(f"   📊 Overall diagnosis: {summary['overall_diagnosis']}")
    print(f"   ⚠️  Issues found: {summary['issues_found']}")
    print(f"   💡 Recommendation: {summary['overall_recommendation']}")
    print(f"   📋 Report: {report_path}")
    
    # Print critical issues
    if report["critical_issues"]:
        print(f"\n🚨 Critical Issues:")
        for issue in report["critical_issues"]:
            print(f"   • {issue['test']}: {issue['issue']}")
            print(f"     → {issue['recommendation']}")
            
    # Print emergency fixes if there are issues
    if summary['issues_found'] > 0:
        print(f"\n⚡ Emergency Fixes:")
        for fix in report["emergency_fixes"]:
            print(f"   • {fix}")
    
    return summary['issues_found']  # Return number of issues as exit code

if __name__ == "__main__":
    sys.exit(main())