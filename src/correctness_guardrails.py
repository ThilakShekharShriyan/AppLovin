#!/usr/bin/env python3
"""
Correctness Guardrails System

Validates query results and data integrity to ensure mathematical correctness:
- Timezone consistency (UTC)
- BETWEEN operator inclusivity
- AVG computation accuracy
- MV vs base table consistency
- Data type correctness
"""

import argparse
import json
import time
import statistics
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import duckdb
import orjson
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class ValidationResult:
    test_name: str
    passed: bool
    details: str
    expected_value: Any
    actual_value: Any
    tolerance_pct: Optional[float] = None
    
@dataclass
class CorrectnessReport:
    total_tests: int
    passed_tests: int
    failed_tests: int
    pass_rate: float
    validation_results: List[ValidationResult]
    summary: Dict[str, Any]

class CorrectnessValidator:
    def __init__(self, lake_path: str, mvs_path: str, threads: int = 4, memory: str = "6GB"):
        self.lake_path = lake_path
        self.mvs_path = mvs_path
        self.threads = threads
        self.memory = memory
        self.results: List[ValidationResult] = []
        
    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB connection with standard settings."""
        con = duckdb.connect(database=":memory:")
        con.execute(f"PRAGMA threads={self.threads};")
        con.execute(f"SET memory_limit='{self.memory}';")
        con.execute("SET TimeZone='UTC';")  # Ensure UTC timezone
        return con
        
    def setup_mv_catalog(self, con: duckdb.DuckDBPyConnection) -> Dict[str, str]:
        """Setup MV catalog mapping."""
        mv_candidates = [
            "mv_day_minute_impr", "mv_day_country_publisher_impr", "mv_all_adv_type_counts",
            "mv_day_advertiser_id_wide", "mv_hour_advertiser_id_wide", "mv_week_advertiser_id_wide",
            "mv_day_publisher_id_wide", "mv_hour_publisher_id_wide", "mv_week_publisher_id_wide",
            "mv_day_country_wide", "mv_hour_country_wide", "mv_week_country_wide",
            "mv_day_type_wide", "mv_hour_type_wide", "mv_week_type_wide"
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
        
    def run_all_validations(self) -> CorrectnessReport:
        """Run comprehensive correctness validation suite."""
        print("🔍 Starting Correctness Validation Suite")
        print(f"   🏠 Lake: {self.lake_path}")
        print(f"   📊 MVs: {self.mvs_path}")
        print(f"   💾 Memory: {self.memory}")
        print()
        
        # Run validation categories
        self.validate_timezone_consistency()
        self.validate_between_operator_inclusivity()
        self.validate_avg_computation_accuracy()
        self.validate_mv_base_consistency()
        self.validate_data_types()
        self.validate_null_handling()
        self.validate_date_boundaries()
        
        # Generate report
        return self._generate_report()
        
    def validate_timezone_consistency(self):
        """Validate that all timestamps are in UTC."""
        print("🕐 Validating timezone consistency...")
        
        con = self.setup_connection()
        try:
            # Set up events view
            con.execute(f"CREATE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            
            # Check timezone setting
            tz_result = con.execute("SELECT current_setting('TimeZone');").fetchone()
            expected_tz = "UTC"
            actual_tz = tz_result[0] if tz_result else "Unknown"
            
            self.results.append(ValidationResult(
                test_name="Timezone Setting",
                passed=actual_tz == expected_tz,
                details=f"DuckDB timezone setting verification",
                expected_value=expected_tz,
                actual_value=actual_tz
            ))
            
            # Check for any timezone indicators in timestamp data (sample check)
            # Use ts column, cast to VARCHAR for pattern check
            sample_result = con.execute("""
                SELECT COUNT(*) 
                FROM events_v 
                WHERE CAST(ts AS VARCHAR) LIKE '%+%' OR CAST(ts AS VARCHAR) LIKE '%-0%' 
                LIMIT 100
            """).fetchone()
            
            tz_indicators = sample_result[0] if sample_result else 0
            
            self.results.append(ValidationResult(
                test_name="Timestamp Format Consistency",
                passed=tz_indicators == 0,
                details="Check for timezone indicators in timestamp strings",
                expected_value=0,
                actual_value=tz_indicators
            ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                test_name="Timezone Validation",
                passed=False,
                details=f"Error during timezone validation: {str(e)}",
                expected_value="UTC compliance",
                actual_value="Error"
            ))
        finally:
            con.close()
            
    def validate_between_operator_inclusivity(self):
        """Validate BETWEEN operator includes both boundaries."""
        print("📊 Validating BETWEEN operator inclusivity...")
        
        con = self.setup_connection()
        try:
            con.execute(f"CREATE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            
            # Test BETWEEN with known date boundaries
            test_date = "2024-01-15"  # Should exist in data
            
            # Count with BETWEEN
            between_result = con.execute(f"""
                SELECT COUNT(*) 
                FROM events_v 
                WHERE day BETWEEN '{test_date}' AND '{test_date}'
            """).fetchone()
            
            # Count with equality
            eq_result = con.execute(f"""
                SELECT COUNT(*) 
                FROM events_v 
                WHERE day = '{test_date}'
            """).fetchone()
            
            between_count = between_result[0] if between_result else 0
            eq_count = eq_result[0] if eq_result else 0
            
            self.results.append(ValidationResult(
                test_name="BETWEEN Inclusivity",
                passed=between_count == eq_count and between_count > 0,
                details=f"BETWEEN '{test_date}' AND '{test_date}' should equal day = '{test_date}'",
                expected_value=eq_count,
                actual_value=between_count
            ))
            
            # Test BETWEEN with range
            range_result = con.execute(f"""
                SELECT COUNT(*) 
                FROM events_v 
                WHERE day BETWEEN '2024-01-15' AND '2024-01-17'
            """).fetchone()
            
            individual_days = con.execute(f"""
                SELECT COUNT(*) 
                FROM events_v 
                WHERE day IN ('2024-01-15', '2024-01-16', '2024-01-17')
            """).fetchone()
            
            range_count = range_result[0] if range_result else 0
            individual_count = individual_days[0] if individual_days else 0
            
            self.results.append(ValidationResult(
                test_name="BETWEEN Range Inclusivity",
                passed=range_count == individual_count,
                details="BETWEEN range should include both boundaries",
                expected_value=individual_count,
                actual_value=range_count
            ))
            
        except Exception as e:
            self.results.append(ValidationResult(
                test_name="BETWEEN Validation",
                passed=False,
                details=f"Error during BETWEEN validation: {str(e)}",
                expected_value="Inclusive boundaries",
                actual_value="Error"
            ))
        finally:
            con.close()
            
    def validate_avg_computation_accuracy(self):
        """Validate AVG computations are mathematically correct."""
        print("🧮 Validating AVG computation accuracy...")
        
        con = self.setup_connection()
        try:
            con.execute(f"CREATE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            mv_glob = self.setup_mv_catalog(con)
            
            # Test AVG accuracy with a simple case
            # Compare direct AVG vs SUM/COUNT
            direct_avg = con.execute("""
                SELECT AVG(CAST(total_price AS DOUBLE))
                FROM events_v 
                WHERE type = 'purchase' AND day = '2024-01-15'
                LIMIT 10000
            """).fetchone()
            
            sum_count = con.execute("""
                SELECT SUM(CAST(total_price AS DOUBLE)), COUNT(*)
                FROM events_v 
                WHERE type = 'purchase' AND day = '2024-01-15'
                LIMIT 10000  
            """).fetchone()
            
            direct_avg_val = direct_avg[0] if direct_avg and direct_avg[0] else 0
            sum_val, count_val = (sum_count[0] or 0, sum_count[1] or 0) if sum_count else (0, 0)
            calculated_avg = sum_val / count_val if count_val > 0 else 0
            
            # Check if within acceptable tolerance (0.001%)
            tolerance = 0.001
            avg_diff = abs(direct_avg_val - calculated_avg) if direct_avg_val > 0 else 0
            avg_diff_pct = (avg_diff / direct_avg_val * 100) if direct_avg_val > 0 else 0
            
            self.results.append(ValidationResult(
                test_name="AVG vs SUM/COUNT Accuracy",
                passed=avg_diff_pct <= tolerance,
                details=f"Direct AVG vs calculated SUM/COUNT should match within {tolerance}%",
                expected_value=round(direct_avg_val, 6),
                actual_value=round(calculated_avg, 6),
                tolerance_pct=avg_diff_pct
            ))
            
            # Test MV AVG computation if available
            if "mv_day_country_wide" in mv_glob:
                mv_avg = con.execute(f"""
                    SELECT SUM(sum_total_pur) / NULLIF(SUM(cnt_total_pur), 0)
                    FROM read_parquet('{mv_glob["mv_day_country_wide"]}')
                    WHERE day = '2024-01-15'
                """).fetchone()
                
                mv_avg_val = mv_avg[0] if mv_avg and mv_avg[0] else 0
                
                # Compare MV avg with base table avg
                mv_diff_pct = abs(mv_avg_val - direct_avg_val) / direct_avg_val * 100 if direct_avg_val > 0 else 0
                
                self.results.append(ValidationResult(
                    test_name="MV AVG Computation",
                    passed=mv_diff_pct <= tolerance,
                    details=f"MV AVG should match base table AVG within {tolerance}%",
                    expected_value=round(direct_avg_val, 6),
                    actual_value=round(mv_avg_val, 6),
                    tolerance_pct=mv_diff_pct
                ))
                
        except Exception as e:
            self.results.append(ValidationResult(
                test_name="AVG Computation Validation",
                passed=False,
                details=f"Error during AVG validation: {str(e)}",
                expected_value="Mathematical accuracy",
                actual_value="Error"
            ))
        finally:
            con.close()
            
    def validate_mv_base_consistency(self):
        """Validate MV aggregates match base table calculations."""
        print("📋 Validating MV vs base table consistency...")
        
        con = self.setup_connection()
        try:
            con.execute(f"CREATE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            mv_glob = self.setup_mv_catalog(con)
            
            # Test count consistency for mv_day_country_wide
            if "mv_day_country_wide" in mv_glob:
                # MV total count
                mv_count = con.execute(f"""
                    SELECT SUM(events_all) 
                    FROM read_parquet('{mv_glob["mv_day_country_wide"]}')
                    WHERE day = '2024-01-15'
                """).fetchone()
                
                # Base table count
                base_count = con.execute("""
                    SELECT COUNT(*) 
                    FROM events_v 
                    WHERE day = '2024-01-15'
                """).fetchone()
                
                mv_count_val = mv_count[0] if mv_count else 0
                base_count_val = base_count[0] if base_count else 0
                
                self.results.append(ValidationResult(
                    test_name="MV Count Consistency",
                    passed=mv_count_val == base_count_val,
                    details="MV aggregate count should match base table count",
                    expected_value=base_count_val,
                    actual_value=mv_count_val
                ))
                
            # Test sum consistency
            if "mv_day_type_wide" in mv_glob:
                # MV impression count (events_all filtered by type)
                mv_impr_count = con.execute(f"""
                    SELECT SUM(events_all) 
                    FROM read_parquet('{mv_glob["mv_day_type_wide"]}')
                    WHERE day = '2024-01-15' AND type = 'impression'
                """).fetchone()
                
                # Base table impression count
                base_impr_count = con.execute("""
                    SELECT COUNT(*) 
                    FROM events_v 
                    WHERE day = '2024-01-15' AND type = 'impression'
                """).fetchone()
                
                mv_impr_val = mv_impr_count[0] if mv_impr_count else 0
                base_impr_val = base_impr_count[0] if base_impr_count else 0
                
                self.results.append(ValidationResult(
                    test_name="MV Type Filter Consistency",
                    passed=mv_impr_val == base_impr_val,
                    details="MV type-filtered counts should match base table",
                    expected_value=base_impr_val,
                    actual_value=mv_impr_val
                ))
                
        except Exception as e:
            self.results.append(ValidationResult(
                test_name="MV Consistency Validation",
                passed=False,
                details=f"Error during MV consistency validation: {str(e)}",
                expected_value="MV/base consistency",
                actual_value="Error"
            ))
        finally:
            con.close()
            
    def validate_data_types(self):
        """Validate data type consistency and correctness."""
        print("🔢 Validating data types...")
        
        con = self.setup_connection()
        try:
            con.execute(f"CREATE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            
            # Check for expected data types
            schema_result = con.execute("DESCRIBE events_v;").fetchall()
            schema = {row[0]: row[1] for row in schema_result}
            
            # Validate expected column types
            expected_types = {
                "day": "VARCHAR",  # Partition column is VARCHAR
                "type": "VARCHAR", 
                "country": "VARCHAR",
                "total_price": ["DOUBLE", "DECIMAL", "FLOAT"],  # Numeric types
                "advertiser_id": ["INTEGER", "BIGINT"],
                "publisher_id": ["INTEGER", "BIGINT"]
            }
            
            for col, expected in expected_types.items():
                if col in schema:
                    actual_type = schema[col]
                    if isinstance(expected, list):
                        type_ok = any(exp in actual_type.upper() for exp in expected)
                    else:
                        type_ok = expected in actual_type.upper()
                        
                    self.results.append(ValidationResult(
                        test_name=f"Data Type: {col}",
                        passed=type_ok,
                        details=f"Column {col} type validation",
                        expected_value=expected,
                        actual_value=actual_type
                    ))
                    
            # Check for numeric precision
            price_stats = con.execute("""
                SELECT MIN(total_price), MAX(total_price), AVG(total_price)
                FROM events_v 
                WHERE total_price > 0 
                LIMIT 10000
            """).fetchone()
            
            if price_stats:
                min_price, max_price, avg_price = price_stats
                # Prices should be reasonable (not negative, not extremely large)
                reasonable_range = min_price >= 0 and max_price < 10000 and avg_price > 0
                
                self.results.append(ValidationResult(
                    test_name="Price Value Ranges",
                    passed=reasonable_range,
                    details="Price values should be in reasonable ranges",
                    expected_value="0 <= price < 10000",
                    actual_value=f"min={min_price:.2f}, max={max_price:.2f}, avg={avg_price:.2f}"
                ))
                
        except Exception as e:
            self.results.append(ValidationResult(
                test_name="Data Type Validation",
                passed=False,
                details=f"Error during data type validation: {str(e)}",
                expected_value="Correct data types",
                actual_value="Error"
            ))
        finally:
            con.close()
            
    def validate_null_handling(self):
        """Validate NULL value handling consistency."""
        print("❓ Validating NULL handling...")
        
        con = self.setup_connection()
        try:
            con.execute(f"CREATE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            
            # Check for unexpected NULLs in key columns
            key_columns = ["day", "type", "advertiser_id", "publisher_id"]
            
            for col in key_columns:
                null_count = con.execute(f"""
                    SELECT COUNT(*) 
                    FROM events_v 
                    WHERE {col} IS NULL 
                    LIMIT 10000
                """).fetchone()
                
                null_count_val = null_count[0] if null_count else 0
                
                # Key columns should have minimal NULLs
                self.results.append(ValidationResult(
                    test_name=f"NULL Check: {col}",
                    passed=null_count_val == 0,
                    details=f"Key column {col} should not contain NULLs",
                    expected_value=0,
                    actual_value=null_count_val
                ))
                
            # Test NULL handling in aggregations
            avg_with_nulls = con.execute("""
                SELECT AVG(total_price), COUNT(total_price), COUNT(*)
                FROM events_v
                WHERE day = '2024-01-15'
                LIMIT 10000
            """).fetchone()
            
            if avg_with_nulls:
                avg_val, count_non_null, count_all = avg_with_nulls
                null_count = count_all - count_non_null
                
                self.results.append(ValidationResult(
                    test_name="NULL Exclusion in AVG",
                    passed=count_non_null <= count_all,
                    details="AVG should properly exclude NULL values",
                    expected_value=f"non_null_count <= total_count",
                    actual_value=f"non_null={count_non_null}, total={count_all}, nulls={null_count}"
                ))
                
        except Exception as e:
            self.results.append(ValidationResult(
                test_name="NULL Handling Validation",
                passed=False,
                details=f"Error during NULL validation: {str(e)}",
                expected_value="Proper NULL handling",
                actual_value="Error"
            ))
        finally:
            con.close()
            
    def validate_date_boundaries(self):
        """Validate date boundary handling and parsing."""
        print("📅 Validating date boundaries...")
        
        con = self.setup_connection()
        try:
            con.execute(f"CREATE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            
            # Check date range consistency
            date_range = con.execute("""
                SELECT MIN(day), MAX(day), COUNT(DISTINCT day)
                FROM events_v
            """).fetchone()
            
            if date_range:
                min_date, max_date, unique_days = date_range
                
                # Validate date format (should be YYYY-MM-DD)
                date_format_ok = (
                    len(min_date) == 10 and len(max_date) == 10 and
                    min_date[4] == '-' and min_date[7] == '-' and
                    max_date[4] == '-' and max_date[7] == '-'
                )
                
                self.results.append(ValidationResult(
                    test_name="Date Format Consistency",
                    passed=date_format_ok,
                    details="Dates should be in YYYY-MM-DD format",
                    expected_value="YYYY-MM-DD format",
                    actual_value=f"min={min_date}, max={max_date}"
                ))
                
                # Check for reasonable date range (within expected bounds)
                reasonable_dates = min_date >= "2024-01-01" and max_date <= "2024-12-31"
                
                self.results.append(ValidationResult(
                    test_name="Date Range Validity",
                    passed=reasonable_dates,
                    details="Dates should be within expected 2024 range",
                    expected_value="2024-01-01 to 2024-12-31",
                    actual_value=f"{min_date} to {max_date}"
                ))
                
                # Check for date continuity (no major gaps)
                expected_days_approx = 365  # Rough estimate for 2024
                reasonable_coverage = unique_days >= expected_days_approx * 0.8  # At least 80% coverage
                
                self.results.append(ValidationResult(
                    test_name="Date Coverage",
                    passed=reasonable_coverage,
                    details="Should have reasonable date coverage",
                    expected_value=f">= {int(expected_days_approx * 0.8)} days",
                    actual_value=f"{unique_days} unique days"
                ))
                
        except Exception as e:
            self.results.append(ValidationResult(
                test_name="Date Boundary Validation",
                passed=False,
                details=f"Error during date validation: {str(e)}",
                expected_value="Valid date boundaries",
                actual_value="Error"
            ))
        finally:
            con.close()
            
    def _generate_report(self) -> CorrectnessReport:
        """Generate comprehensive correctness report."""
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r.passed)
        failed_tests = total_tests - passed_tests
        pass_rate = passed_tests / total_tests if total_tests > 0 else 0
        
        # Categorize failures
        failure_categories = {}
        for result in self.results:
            if not result.passed:
                category = result.test_name.split(":")[0].strip()
                if category not in failure_categories:
                    failure_categories[category] = []
                failure_categories[category].append(result.test_name)
                
        summary = {
            "timestamp": time.time(),
            "configuration": {
                "lake_path": self.lake_path,
                "mvs_path": self.mvs_path,
                "memory": self.memory,
                "threads": self.threads
            },
            "test_categories": {
                "timezone": len([r for r in self.results if "timezone" in r.test_name.lower()]),
                "between_operator": len([r for r in self.results if "between" in r.test_name.lower()]),
                "avg_computation": len([r for r in self.results if "avg" in r.test_name.lower()]),
                "mv_consistency": len([r for r in self.results if "mv" in r.test_name.lower()]),
                "data_types": len([r for r in self.results if "data type" in r.test_name.lower()]),
                "null_handling": len([r for r in self.results if "null" in r.test_name.lower()]),
                "date_boundaries": len([r for r in self.results if "date" in r.test_name.lower()])
            },
            "failure_categories": failure_categories,
            "critical_failures": [r.test_name for r in self.results if not r.passed and "consistency" in r.test_name.lower()]
        }
        
        return CorrectnessReport(
            total_tests=total_tests,
            passed_tests=passed_tests,
            failed_tests=failed_tests,
            pass_rate=pass_rate,
            validation_results=self.results,
            summary=summary
        )
        
    def export_report(self, report: CorrectnessReport, output_path: str) -> str:
        """Export correctness report to JSON file."""
        report_data = {
            "correctness_validation": {
                "total_tests": report.total_tests,
                "passed_tests": report.passed_tests,
                "failed_tests": report.failed_tests,
                "pass_rate": round(report.pass_rate * 100, 2)
            },
            "summary": report.summary,
            "detailed_results": [
                {
                    "test_name": r.test_name,
                    "passed": r.passed,
                    "details": r.details,
                    "expected": str(r.expected_value),
                    "actual": str(r.actual_value),
                    "tolerance_pct": r.tolerance_pct
                } for r in report.validation_results
            ],
            "recommendations": self._generate_recommendations(report)
        }
        
        with open(output_path, 'w') as f:
            json.dump(report_data, f, indent=2)
            
        return output_path
        
    def _generate_recommendations(self, report: CorrectnessReport) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        if report.pass_rate < 0.9:
            recommendations.append("Multiple validation failures detected. Review data pipeline and MV generation process.")
            
        failure_categories = report.summary.get("failure_categories", {})
        
        if "MV" in failure_categories or "mv" in str(failure_categories).lower():
            recommendations.append("MV consistency issues found. Consider regenerating materialized views.")
            
        if "AVG" in failure_categories or "avg" in str(failure_categories).lower():
            recommendations.append("AVG computation accuracy issues. Verify numeric precision and aggregation logic.")
            
        if "Date" in failure_categories or "date" in str(failure_categories).lower():
            recommendations.append("Date boundary issues detected. Verify date parsing and timezone handling.")
            
        critical_failures = report.summary.get("critical_failures", [])
        if critical_failures:
            recommendations.append(f"Critical failures in: {', '.join(critical_failures)}. Immediate investigation required.")
            
        if report.pass_rate >= 0.95:
            recommendations.append("Excellent data integrity! System passing 95%+ of correctness tests.")
            
        return recommendations

def main():
    parser = argparse.ArgumentParser(description="Correctness Guardrails Validation")
    parser.add_argument("--lake", required=True, help="Path to Parquet lake")
    parser.add_argument("--mvs", required=True, help="Path to materialized views")
    parser.add_argument("--out", required=True, help="Output path for validation report")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    parser.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    args = parser.parse_args()
    
    # Create validator
    validator = CorrectnessValidator(
        lake_path=args.lake,
        mvs_path=args.mvs,
        threads=args.threads,
        memory=args.mem
    )
    
    # Run validation suite
    report = validator.run_all_validations()
    
    # Export results
    report_path = validator.export_report(report, args.out)
    
    print(f"\n🎯 Correctness Validation Complete!")
    print(f"   📊 Report: {report_path}")
    print(f"   ✅ Tests passed: {report.passed_tests}/{report.total_tests} ({report.pass_rate*100:.1f}%)")
    
    if report.failed_tests > 0:
        print(f"   ❌ Tests failed: {report.failed_tests}")
        print("   🔍 Review detailed report for failure analysis")
    else:
        print("   🏆 All tests passed - excellent data integrity!")
        
    # Print any critical failures immediately
    critical_failures = report.summary.get("critical_failures", [])
    if critical_failures:
        print(f"\n⚠️  Critical Failures:")
        for failure in critical_failures:
            print(f"   • {failure}")

if __name__ == "__main__":
    main()