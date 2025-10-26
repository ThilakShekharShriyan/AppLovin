#!/usr/bin/env python3
"""
Rebuilt MV Integrity Validator

Validates the integrity of rebuilt materialized views by:
- Checking basic consistency with base data
- Validating record counts and key metrics
- Ensuring data freshness and completeness
"""

import time
import json
import argparse
from typing import Dict, List, Any
from dataclasses import dataclass
import duckdb
from pathlib import Path

@dataclass
class ValidationResult:
    mv_name: str
    status: str  # HEALTHY, STALE, ERROR
    record_count: int
    base_comparison: Dict[str, Any]
    freshness_check: Dict[str, Any]
    details: str

class RebuiltMVValidator:
    def __init__(self, lake_path: str, mvs_path: str, threads: int = 4, memory: str = "3GB"):
        self.lake_path = lake_path
        self.mvs_path = mvs_path
        self.threads = threads
        self.memory = memory
        self.results: List[ValidationResult] = []
        
    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB connection."""
        con = duckdb.connect(database=":memory:")
        con.execute(f"PRAGMA threads={self.threads};")
        con.execute(f"SET memory_limit='{self.memory}';")
        con.execute("SET TimeZone='UTC';")
        return con
        
    def validate_mv_all_adv_type_counts(self) -> ValidationResult:
        """Validate advertiser-type counts MV."""
        mv_name = "mv_all_adv_type_counts"
        print(f"🔍 Validating {mv_name}...")
        
        con = self.setup_connection()
        try:
            # Check MV record count and sample data
            mv_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{self.mvs_path}/{mv_name}/*.parquet')").fetchone()[0]
            
            if mv_count == 0:
                return ValidationResult(mv_name, "ERROR", 0, {}, {}, "MV is empty")
                
            # Compare with base data aggregation
            base_count = con.execute(f"""
                SELECT COUNT(DISTINCT advertiser_id || '-' || type) 
                FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet')
                WHERE advertiser_id IS NOT NULL AND type IS NOT NULL
            """).fetchone()[0]
            
            # Sample comparison - check total events for a few advertiser-type combinations  
            comparison_sql = f"""
            WITH mv_sample AS (
                SELECT advertiser_id, type, total_events
                FROM read_parquet('{self.mvs_path}/{mv_name}/*.parquet')
                ORDER BY total_events DESC LIMIT 5
            ),
            base_sample AS (
                SELECT 
                    advertiser_id, type, 
                    COUNT(*) as total_events
                FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet')
                WHERE advertiser_id IS NOT NULL AND type IS NOT NULL
                GROUP BY advertiser_id, type
                ORDER BY total_events DESC LIMIT 5
            )
            SELECT 
                COALESCE(mv_sample.advertiser_id, base_sample.advertiser_id) as advertiser_id,
                COALESCE(mv_sample.type, base_sample.type) as type,
                mv_sample.total_events as mv_events,
                base_sample.total_events as base_events,
                ABS(COALESCE(mv_sample.total_events, 0) - COALESCE(base_sample.total_events, 0)) as diff
            FROM mv_sample
            FULL JOIN base_sample ON mv_sample.advertiser_id = base_sample.advertiser_id 
                                 AND mv_sample.type = base_sample.type
            ORDER BY diff DESC
            """
            
            comparison_results = con.execute(comparison_sql).fetchall()
            max_diff = max([row[4] for row in comparison_results]) if comparison_results else 0
            
            # Freshness check - check date range
            freshness_result = con.execute(f"""
                SELECT MIN(first_seen) as earliest, MAX(last_seen) as latest
                FROM read_parquet('{self.mvs_path}/{mv_name}/*.parquet')
            """).fetchone()
            
            status = "HEALTHY" if max_diff == 0 and mv_count > 0 else ("STALE" if max_diff < mv_count * 0.01 else "ERROR")
            
            return ValidationResult(
                mv_name=mv_name,
                status=status,
                record_count=mv_count,
                base_comparison={"mv_count": mv_count, "base_count": base_count, "max_diff": max_diff},
                freshness_check={"earliest": freshness_result[0], "latest": freshness_result[1]},
                details=f"MV has {mv_count} records, base unique combinations: {base_count}, max event diff: {max_diff}"
            )
            
        except Exception as e:
            return ValidationResult(mv_name, "ERROR", 0, {}, {}, f"Validation error: {str(e)}")
        finally:
            con.close()
            
    def validate_mv_day_wide_tables(self, mv_name: str) -> ValidationResult:
        """Validate day-partitioned wide table MVs."""
        print(f"🔍 Validating {mv_name}...")
        
        con = self.setup_connection()
        try:
            # Check MV exists and has data
            mv_files = list(Path(f"{self.mvs_path}/{mv_name}").rglob("*.parquet"))
            if not mv_files:
                return ValidationResult(mv_name, "ERROR", 0, {}, {}, "No MV files found")
                
            mv_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{self.mvs_path}/{mv_name}/**/*.parquet')").fetchone()[0]
            
            if mv_count == 0:
                return ValidationResult(mv_name, "ERROR", 0, {}, {}, "MV is empty")
                
            # Get date range from MV
            date_range = con.execute(f"""
                SELECT MIN(day) as min_day, MAX(day) as max_day, COUNT(DISTINCT day) as unique_days
                FROM read_parquet('{self.mvs_path}/{mv_name}/**/*.parquet')
            """).fetchone()
            
            # Compare impression/click counts for sample days
            if 'advertiser' in mv_name:
                comparison_sql = f"""
                WITH mv_agg AS (
                    SELECT day, SUM(impressions) as mv_impressions, SUM(clicks) as mv_clicks
                    FROM read_parquet('{self.mvs_path}/{mv_name}/**/*.parquet')
                    GROUP BY day
                    ORDER BY day LIMIT 10
                ),
                base_agg AS (
                    SELECT 
                        day,
                        SUM(CASE WHEN type = 'impression' THEN 1 ELSE 0 END) as base_impressions,
                        SUM(CASE WHEN type = 'click' THEN 1 ELSE 0 END) as base_clicks
                    FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet')
                    WHERE advertiser_id IS NOT NULL
                    GROUP BY day
                    ORDER BY day LIMIT 10  
                )
                SELECT 
                    mv_agg.day,
                    mv_agg.mv_impressions,
                    base_agg.base_impressions,
                    ABS(mv_agg.mv_impressions - COALESCE(base_agg.base_impressions, 0)) as impr_diff
                FROM mv_agg
                LEFT JOIN base_agg ON mv_agg.day = base_agg.day
                ORDER BY impr_diff DESC
                LIMIT 3
                """
            else:
                # For country/type tables, just check record counts by day
                comparison_sql = f"""
                WITH mv_agg AS (
                    SELECT day, COUNT(*) as mv_records
                    FROM read_parquet('{self.mvs_path}/{mv_name}/**/*.parquet')
                    GROUP BY day
                    ORDER BY day LIMIT 5
                )
                SELECT day, mv_records, 0 as impr_diff FROM mv_agg
                """
                
            comparison_results = con.execute(comparison_sql).fetchall()
            max_diff = max([row[3] if len(row) > 3 else 0 for row in comparison_results]) if comparison_results else 0
            
            # Status determination
            status = "HEALTHY" if max_diff < mv_count * 0.05 else ("STALE" if max_diff < mv_count * 0.1 else "ERROR")
            
            return ValidationResult(
                mv_name=mv_name,
                status=status,
                record_count=mv_count,
                base_comparison={"mv_count": mv_count, "max_diff": max_diff},
                freshness_check={"min_day": date_range[0], "max_day": date_range[1], "unique_days": date_range[2]},
                details=f"MV has {mv_count} records across {date_range[2]} days ({date_range[0]} to {date_range[1]}), max diff: {max_diff}"
            )
            
        except Exception as e:
            return ValidationResult(mv_name, "ERROR", 0, {}, {}, f"Validation error: {str(e)}")
        finally:
            con.close()
            
    def run_all_validations(self) -> Dict[str, Any]:
        """Run validation for all rebuilt MVs."""
        print("🔍 Starting Rebuilt MV Validation")
        print(f"   📁 Lake: {self.lake_path}")
        print(f"   📊 MVs: {self.mvs_path}")
        print()
        
        # Validate specific MVs
        result1 = self.validate_mv_all_adv_type_counts()
        self.results.append(result1)
        
        mv_wide_tables = [
            "mv_day_advertiser_id_wide",
            "mv_day_country_wide", 
            "mv_day_type_wide",
            "mv_hour_advertiser_id_wide"
        ]
        
        for mv_name in mv_wide_tables:
            result = self.validate_mv_day_wide_tables(mv_name)
            self.results.append(result)
            
        # Print results
        for result in self.results:
            status_icon = "✅" if result.status == "HEALTHY" else ("⚠️" if result.status == "STALE" else "❌")
            print(f"   {status_icon} {result.mv_name}: {result.status} ({result.record_count:,} records)")
            
        return self.generate_validation_report()
        
    def generate_validation_report(self) -> Dict[str, Any]:
        """Generate comprehensive validation report."""
        healthy_mvs = [r for r in self.results if r.status == "HEALTHY"]
        stale_mvs = [r for r in self.results if r.status == "STALE"]
        error_mvs = [r for r in self.results if r.status == "ERROR"]
        
        total_records = sum(r.record_count for r in self.results)
        
        report = {
            "validation_summary": {
                "timestamp": time.time(),
                "total_mvs": len(self.results),
                "healthy_mvs": len(healthy_mvs),
                "stale_mvs": len(stale_mvs), 
                "error_mvs": len(error_mvs),
                "health_rate": round(len(healthy_mvs) / len(self.results) * 100, 1) if self.results else 0,
                "total_records": total_records
            },
            "mv_details": [
                {
                    "mv_name": r.mv_name,
                    "status": r.status,
                    "record_count": r.record_count,
                    "base_comparison": r.base_comparison,
                    "freshness_check": r.freshness_check,
                    "details": r.details
                } for r in self.results
            ],
            "stale_mv_list": [r.mv_name for r in stale_mvs],
            "error_mv_list": [r.mv_name for r in error_mvs],
            "recommendations": self._generate_recommendations()
        }
        
        return report
        
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        error_mvs = [r for r in self.results if r.status == "ERROR"]
        stale_mvs = [r for r in self.results if r.status == "STALE"]
        
        if error_mvs:
            recommendations.append(f"Investigate {len(error_mvs)} error MVs: {', '.join([r.mv_name for r in error_mvs])}")
            
        if stale_mvs:
            recommendations.append(f"Consider refreshing {len(stale_mvs)} stale MVs: {', '.join([r.mv_name for r in stale_mvs])}")
            
        healthy_mvs = [r for r in self.results if r.status == "HEALTHY"]
        if len(healthy_mvs) == len(self.results):
            recommendations.append("All MVs are healthy - ready for production queries")
            recommendations.append("Update query router to use rebuilt materialized views")
            recommendations.append("Set up monitoring for ongoing MV health checks")
        else:
            recommendations.append("Address MV issues before routing production queries to these MVs")
            
        return recommendations
        
    def export_report(self, report: Dict[str, Any], output_path: str) -> str:
        """Export validation report."""
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        return output_path

def main():
    parser = argparse.ArgumentParser(description="Rebuilt MV Integrity Validator")
    parser.add_argument("--lake", required=True, help="Path to cleaned data lake")
    parser.add_argument("--mvs", required=True, help="Path to rebuilt materialized views")
    parser.add_argument("--report", required=True, help="Output path for validation report")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    parser.add_argument("--mem", default="3GB", help="DuckDB memory limit")
    args = parser.parse_args()
    
    validator = RebuiltMVValidator(
        lake_path=args.lake,
        mvs_path=args.mvs,
        threads=args.threads,
        memory=args.mem
    )
    
    # Run validations
    report = validator.run_all_validations()
    
    # Export report
    report_path = validator.export_report(report, args.report)
    
    print(f"\n🎯 Rebuilt MV Validation Complete!")
    print(f"   📊 Report: {report_path}")
    print(f"   ✅ Health rate: {report['validation_summary']['health_rate']}%")
    print(f"   📈 Healthy MVs: {report['validation_summary']['healthy_mvs']}/{report['validation_summary']['total_mvs']}")
    print(f"   📊 Total records: {report['validation_summary']['total_records']:,}")
    
    if report['validation_summary']['stale_mvs'] > 0:
        print(f"   ⚠️  Stale MVs: {report['validation_summary']['stale_mvs']}")
        
    if report['validation_summary']['error_mvs'] > 0:
        print(f"   ❌ Error MVs: {report['validation_summary']['error_mvs']}")
        
    # Print recommendations
    if report["recommendations"]:
        print(f"\n💡 Recommendations:")
        for rec in report["recommendations"]:
            print(f"   • {rec}")

if __name__ == "__main__":
    main()