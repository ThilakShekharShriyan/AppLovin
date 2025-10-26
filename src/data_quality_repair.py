#!/usr/bin/env python3
"""
Data Quality Remediation Tool

Fixes data quality issues identified by correctness validation:
- Quarantines NULL values in key columns
- Repairs date format inconsistencies
- Enforces data constraints
- Provides detailed remediation reports
"""

import argparse
import json
import time
import re
import shutil
from pathlib import Path
from typing import Dict, List, Any, Tuple
import duckdb
from dataclasses import dataclass

@dataclass
class RemediationResult:
    issue_type: str
    records_affected: int
    records_quarantined: int
    records_repaired: int
    success: bool
    details: str

class DataQualityRepairer:
    def __init__(self, lake_path: str, output_path: str, threads: int = 4, memory: str = "6GB"):
        self.lake_path = lake_path
        self.output_path = output_path
        self.threads = threads
        self.memory = memory
        self.results: List[RemediationResult] = []
        
    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB connection."""
        con = duckdb.connect(database=":memory:")
        con.execute(f"PRAGMA threads={self.threads};")
        con.execute(f"SET memory_limit='{self.memory}';")
        con.execute("SET TimeZone='UTC';")
        return con
        
    def quarantine_null_records(self) -> RemediationResult:
        """Quarantine records with NULL values in key columns."""
        print("🔍 Quarantining NULL records...")
        
        con = self.setup_connection()
        try:
            # Setup source data view
            con.execute(f"CREATE VIEW events_raw AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            
            # Count total records
            total_count = con.execute("SELECT COUNT(*) FROM events_raw").fetchone()[0]
            
            # Identify bad records (NULL in key columns)
            bad_records_sql = """
            CREATE TABLE events_bad AS
            SELECT *, 'NULL_KEY_COLUMN' as quarantine_reason
            FROM events_raw
            WHERE day IS NULL 
               OR type IS NULL 
               OR advertiser_id IS NULL 
               OR publisher_id IS NULL
            """
            con.execute(bad_records_sql)
            
            bad_count = con.execute("SELECT COUNT(*) FROM events_bad").fetchone()[0]
            
            # Create clean dataset
            clean_records_sql = """
            CREATE TABLE events_clean AS
            SELECT * FROM events_raw
            WHERE day IS NOT NULL 
              AND type IS NOT NULL 
              AND advertiser_id IS NOT NULL 
              AND publisher_id IS NOT NULL
            """
            con.execute(clean_records_sql)
            
            clean_count = con.execute("SELECT COUNT(*) FROM events_clean").fetchone()[0]
            
            # Export quarantined records for analysis
            quarantine_path = Path(self.output_path) / "data_quality" / "quarantined_nulls.csv"
            quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            
            if bad_count > 0:
                con.execute(f"""
                COPY (
                    SELECT *, current_timestamp() as quarantined_at
                    FROM events_bad 
                    ORDER BY day, type, advertiser_id
                    LIMIT 10000
                ) TO '{quarantine_path}' WITH (HEADER, DELIMITER ',')
                """)
                
            # Write clean data back to lake
            clean_output_path = Path(self.output_path) / "events_clean"
            clean_output_path.mkdir(parents=True, exist_ok=True)
            
            con.execute(f"""
            COPY (
                SELECT * FROM events_clean 
                ORDER BY type, day, country, publisher_id, advertiser_id, minute
            ) TO '{clean_output_path}' (
                FORMAT PARQUET, 
                PARTITION_BY (type, day), 
                COMPRESSION ZSTD, 
                ROW_GROUP_SIZE 131072
            )
            """)
            
            return RemediationResult(
                issue_type="NULL_KEY_COLUMNS",
                records_affected=total_count,
                records_quarantined=bad_count,
                records_repaired=clean_count,
                success=True,
                details=f"Quarantined {bad_count} records with NULL key columns. Clean dataset: {clean_count} records."
            )
            
        except Exception as e:
            return RemediationResult(
                issue_type="NULL_KEY_COLUMNS",
                records_affected=0,
                records_quarantined=0,
                records_repaired=0,
                success=False,
                details=f"Error during NULL quarantine: {str(e)}"
            )
        finally:
            con.close()
            
    def repair_date_formats(self) -> RemediationResult:
        """Repair date format inconsistencies in partition structure."""
        print("📅 Repairing date format inconsistencies...")
        
        try:
            events_root = Path(self.lake_path) / "events"
            if not events_root.exists():
                return RemediationResult(
                    issue_type="DATE_FORMAT_REPAIR",
                    records_affected=0,
                    records_quarantined=0,
                    records_repaired=0,
                    success=False,
                    details=f"Events directory not found: {events_root}"
                )
                
            repaired_count = 0
            total_partitions = 0
            
            # Find all day=* partitions
            for partition_dir in events_root.rglob("day=*"):
                if not partition_dir.is_dir():
                    continue
                    
                total_partitions += 1
                partition_value = partition_dir.name.split("=", 1)[1]
                
                # Try to normalize date format
                normalized_date = self._normalize_date_format(partition_value)
                
                if normalized_date and normalized_date != partition_value:
                    # Rename partition to correct format
                    new_partition_name = f"day={normalized_date}"
                    new_partition_path = partition_dir.parent / new_partition_name
                    
                    if not new_partition_path.exists():
                        partition_dir.rename(new_partition_path)
                        repaired_count += 1
                        print(f"   Renamed: {partition_dir.name} → {new_partition_name}")
                    else:
                        # Merge into existing partition if it already exists
                        self._merge_partitions(partition_dir, new_partition_path)
                        repaired_count += 1
                        print(f"   Merged: {partition_dir.name} → {new_partition_name}")
                        
            return RemediationResult(
                issue_type="DATE_FORMAT_REPAIR",
                records_affected=total_partitions,
                records_quarantined=0,
                records_repaired=repaired_count,
                success=True,
                details=f"Repaired {repaired_count} out of {total_partitions} date partitions to YYYY-MM-DD format."
            )
            
        except Exception as e:
            return RemediationResult(
                issue_type="DATE_FORMAT_REPAIR", 
                records_affected=0,
                records_quarantined=0,
                records_repaired=0,
                success=False,
                details=f"Error during date format repair: {str(e)}"
            )
            
    def _normalize_date_format(self, date_value: str) -> str:
        """Normalize various date formats to YYYY-MM-DD."""
        # Handle URL-encoded dates first
        date_value = date_value.replace("%20", " ").replace("%3A", ":")
        
        # Try various date patterns
        patterns = [
            # Already correct format
            r"^(\d{4})-(\d{2})-(\d{2})$",
            # Various separators
            r"^(\d{4})[/_](\d{1,2})[/_](\d{1,2})$",
            # No separators
            r"^(\d{4})(\d{2})(\d{2})$",
            # With time components (extract date part)
            r"^(\d{4})-(\d{2})-(\d{2})\s+.*$",
            r"^(\d{4})[/_](\d{1,2})[/_](\d{1,2})\s+.*$",
        ]
        
        for pattern in patterns:
            match = re.match(pattern, date_value.strip())
            if match:
                yyyy, mm, dd = match.groups()
                try:
                    # Validate and format
                    year = int(yyyy)
                    month = int(mm)
                    day = int(dd)
                    
                    if 1 <= month <= 12 and 1 <= day <= 31 and 2020 <= year <= 2030:
                        return f"{year:04d}-{month:02d}-{day:02d}"
                except ValueError:
                    continue
                    
        return None
        
    def _merge_partitions(self, source_dir: Path, target_dir: Path):
        """Merge source partition into target partition."""
        try:
            # Move all files from source to target
            for file_path in source_dir.rglob("*.parquet"):
                target_file = target_dir / file_path.relative_to(source_dir)
                target_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Generate unique filename if conflict
                if target_file.exists():
                    counter = 1
                    stem = target_file.stem
                    suffix = target_file.suffix
                    while target_file.exists():
                        target_file = target_file.parent / f"{stem}_{counter}{suffix}"
                        counter += 1
                        
                shutil.move(str(file_path), str(target_file))
                
            # Remove empty source directory
            shutil.rmtree(source_dir, ignore_errors=True)
            
        except Exception as e:
            print(f"Warning: Could not merge {source_dir} into {target_dir}: {e}")
            
    def apply_data_constraints(self) -> RemediationResult:
        """Apply data type and value constraints."""
        print("🔒 Applying data constraints...")
        
        con = self.setup_connection()
        try:
            # Use the clean dataset
            clean_path = Path(self.output_path) / "events_clean"
            if not clean_path.exists():
                return RemediationResult(
                    issue_type="DATA_CONSTRAINTS",
                    records_affected=0,
                    records_quarantined=0,
                    records_repaired=0,
                    success=False,
                    details="Clean dataset not found. Run NULL quarantine first."
                )
                
            con.execute(f"CREATE TABLE events_clean AS SELECT * FROM read_parquet('{clean_path}/**/*.parquet');")
            
            total_before = con.execute("SELECT COUNT(*) FROM events_clean").fetchone()[0]
            
            # Apply constraints and identify violations
            constraint_violations = []
            
            # Type constraint: must be valid event type
            invalid_types = con.execute("""
                SELECT COUNT(*) FROM events_clean 
                WHERE type NOT IN ('impression', 'purchase', 'click', 'serve')
            """).fetchone()[0]
            
            if invalid_types > 0:
                constraint_violations.append(f"Invalid event types: {invalid_types} records")
                
            # Country constraint: must be 2-char country code
            invalid_countries = con.execute("""
                SELECT COUNT(*) FROM events_clean 
                WHERE country IS NOT NULL AND LENGTH(country) != 2
            """).fetchone()[0]
            
            if invalid_countries > 0:
                constraint_violations.append(f"Invalid country codes: {invalid_countries} records")
                
            # Price constraints: must be non-negative and reasonable
            invalid_prices = con.execute("""
                SELECT COUNT(*) FROM events_clean 
                WHERE total_price < 0 OR total_price > 10000
            """).fetchone()[0]
            
            if invalid_prices > 0:
                constraint_violations.append(f"Invalid price values: {invalid_prices} records")
                
            # Create constrained dataset
            con.execute("""
            CREATE TABLE events_constrained AS
            SELECT * FROM events_clean
            WHERE type IN ('impression', 'purchase', 'click', 'serve')
              AND (country IS NULL OR LENGTH(country) = 2)
              AND (total_price IS NULL OR (total_price >= 0 AND total_price <= 10000))
            """)
            
            total_after = con.execute("SELECT COUNT(*) FROM events_constrained").fetchone()[0]
            quarantined_count = total_before - total_after
            
            # Export constrained dataset
            constrained_output_path = Path(self.output_path) / "events_final"
            constrained_output_path.mkdir(parents=True, exist_ok=True)
            
            con.execute(f"""
            COPY (
                SELECT * FROM events_constrained
                ORDER BY type, day, country, publisher_id, advertiser_id
            ) TO '{constrained_output_path}' (
                FORMAT PARQUET,
                PARTITION_BY (type, day),
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE 131072
            )
            """)
            
            # Log constraint violations
            violations_log = Path(self.output_path) / "data_quality" / "constraint_violations.json"
            violations_log.parent.mkdir(parents=True, exist_ok=True)
            
            with open(violations_log, 'w') as f:
                json.dump({
                    "timestamp": time.time(),
                    "total_records_before": total_before,
                    "total_records_after": total_after,
                    "quarantined_count": quarantined_count,
                    "violations": constraint_violations
                }, f, indent=2)
                
            return RemediationResult(
                issue_type="DATA_CONSTRAINTS",
                records_affected=total_before,
                records_quarantined=quarantined_count,
                records_repaired=total_after,
                success=True,
                details=f"Applied constraints. Quarantined {quarantined_count} violating records. Final dataset: {total_after} records."
            )
            
        except Exception as e:
            return RemediationResult(
                issue_type="DATA_CONSTRAINTS",
                records_affected=0,
                records_quarantined=0,
                records_repaired=0,
                success=False,
                details=f"Error applying constraints: {str(e)}"
            )
        finally:
            con.close()
            
    def generate_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive data quality remediation report."""
        total_issues = len(self.results)
        resolved_issues = sum(1 for r in self.results if r.success)
        
        total_records_affected = sum(r.records_affected for r in self.results)
        total_quarantined = sum(r.records_quarantined for r in self.results)
        total_repaired = sum(r.records_repaired for r in self.results)
        
        report = {
            "remediation_summary": {
                "timestamp": time.time(),
                "total_issues_addressed": total_issues,
                "successfully_resolved": resolved_issues,
                "resolution_rate": round(resolved_issues / total_issues * 100, 1) if total_issues > 0 else 100,
                "records_affected": total_records_affected,
                "records_quarantined": total_quarantined,
                "records_in_final_dataset": total_repaired
            },
            "remediation_details": [
                {
                    "issue_type": r.issue_type,
                    "success": r.success,
                    "records_affected": r.records_affected,
                    "records_quarantined": r.records_quarantined,
                    "records_repaired": r.records_repaired,
                    "details": r.details
                } for r in self.results
            ],
            "next_steps": self._generate_next_steps()
        }
        
        return report
        
    def _generate_next_steps(self) -> List[str]:
        """Generate next steps based on remediation results."""
        next_steps = []
        
        failed_remediations = [r for r in self.results if not r.success]
        if failed_remediations:
            next_steps.append("Investigate failed remediations and resolve underlying issues.")
            
        total_quarantined = sum(r.records_quarantined for r in self.results)
        if total_quarantined > 0:
            next_steps.append(f"Review {total_quarantined} quarantined records to determine root cause and prevent future issues.")
            
        # Check if we have clean data
        clean_data_available = any(r.issue_type == "DATA_CONSTRAINTS" and r.success for r in self.results)
        if clean_data_available:
            next_steps.append("Rebuild materialized views using the cleaned dataset (events_final).")
            next_steps.append("Update query router to use the cleaned data lake path.")
            next_steps.append("Run MV integrity checks to validate consistency after rebuilding.")
        else:
            next_steps.append("Complete data cleaning process before rebuilding materialized views.")
            
        next_steps.append("Implement data quality checks in the ingestion pipeline to prevent future issues.")
        next_steps.append("Set up monitoring for data quality metrics and constraint violations.")
        
        return next_steps
        
    def run_full_remediation(self) -> Dict[str, Any]:
        """Run complete data quality remediation process."""
        print("🔧 Starting Data Quality Remediation")
        print(f"   📁 Source: {self.lake_path}")
        print(f"   📁 Output: {self.output_path}")
        print()
        
        # Step 1: Quarantine NULL records
        null_result = self.quarantine_null_records()
        self.results.append(null_result)
        
        # Step 2: Repair date formats  
        date_result = self.repair_date_formats()
        self.results.append(date_result)
        
        # Step 3: Apply data constraints
        constraint_result = self.apply_data_constraints()
        self.results.append(constraint_result)
        
        return self.generate_quality_report()
        
    def export_report(self, report: Dict[str, Any], output_path: str) -> str:
        """Export remediation report to JSON file."""
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        return output_path

def main():
    parser = argparse.ArgumentParser(description="Data Quality Remediation Tool")
    parser.add_argument("--lake", required=True, help="Path to source Parquet lake")
    parser.add_argument("--out", required=True, help="Output path for remediated data")
    parser.add_argument("--report", required=True, help="Output path for remediation report")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    parser.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    args = parser.parse_args()
    
    # Create repairer
    repairer = DataQualityRepairer(
        lake_path=args.lake,
        output_path=args.out,
        threads=args.threads,
        memory=args.mem
    )
    
    # Run full remediation
    report = repairer.run_full_remediation()
    
    # Export report
    report_path = repairer.export_report(report, args.report)
    
    print(f"\n🎯 Data Quality Remediation Complete!")
    print(f"   📊 Report: {report_path}")
    print(f"   ✅ Resolution rate: {report['remediation_summary']['resolution_rate']}%")
    print(f"   📈 Final dataset: {report['remediation_summary']['records_in_final_dataset']:,} records")
    
    if report['remediation_summary']['records_quarantined'] > 0:
        print(f"   ⚠️  Quarantined: {report['remediation_summary']['records_quarantined']:,} records")
        
    # Print next steps
    print(f"\n📋 Next Steps:")
    for step in report['next_steps']:
        print(f"   • {step}")

if __name__ == "__main__":
    main()