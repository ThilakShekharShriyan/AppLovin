#!/usr/bin/env python3
"""
MV Integrity Checker

Validates MV freshness and consistency against base tables.
Implements lightweight daily audits with checksums and spot verification.
"""

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import duckdb
from dataclasses import dataclass

@dataclass
class MVHealthCheck:
    mv_name: str
    status: str  # "OK", "STALE", "ERROR"
    checked_at: float
    total_rows: int
    checksum_values: Dict[str, float]
    spot_check_ok: int
    spot_check_mismatched: int
    error_message: Optional[str] = None

class MVIntegrityChecker:
    def __init__(self, lake_path: str, mvs_path: str, threads: int = 4, memory: str = "6GB"):
        self.lake_path = lake_path
        self.mvs_path = mvs_path
        self.threads = threads
        self.memory = memory
        self.health_checks: List[MVHealthCheck] = []
        
    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB connection."""
        con = duckdb.connect(database=":memory:")
        con.execute(f"PRAGMA threads={self.threads};")
        con.execute(f"SET memory_limit='{self.memory}';")
        con.execute("SET TimeZone='UTC';")
        return con
        
    def get_mv_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Define MV validation rules."""
        return {
            "mv_day_country_wide": {
                "base_table": "events_v",
                "key_columns": ["country", "day"],
                "aggregate_columns": {
                    "sum_bid_impr": "SUM(CASE WHEN type='impression' THEN bid_price ELSE 0 END)",
                    "sum_total_pur": "SUM(CASE WHEN type='purchase' THEN total_price ELSE 0 END)",
                    "events_all": "COUNT(*)"
                },
                "sample_size": 100
            },
            "mv_day_type_wide": {
                "base_table": "events_v", 
                "key_columns": ["type", "day"],
                "aggregate_columns": {
                    "sum_bid_impr": "SUM(CASE WHEN type='impression' THEN bid_price ELSE 0 END)",
                    "cnt_impr": "SUM(CASE WHEN type='impression' THEN 1 ELSE 0 END)",
                    "events_all": "COUNT(*)"
                },
                "sample_size": 50
            },
            "mv_day_advertiser_id_wide": {
                "base_table": "events_v",
                "key_columns": ["advertiser_id", "day"],
                "aggregate_columns": {
                    "sum_bid_impr": "SUM(CASE WHEN type='impression' THEN bid_price ELSE 0 END)",
                    "sum_total_pur": "SUM(CASE WHEN type='purchase' THEN total_price ELSE 0 END)",
                    "events_all": "COUNT(*)"
                },
                "sample_size": 75
            },
            "mv_all_adv_type_counts": {
                "base_table": "events_v",
                "key_columns": ["advertiser_id", "type"],
                "aggregate_columns": {
                    "cnt": "COUNT(*)"
                },
                "sample_size": 200
            }
        }
        
    def check_mv_health(self, mv_name: str, mv_def: Dict[str, Any]) -> MVHealthCheck:
        """Perform comprehensive health check on a single MV."""
        print(f"🔍 Checking MV: {mv_name}")
        
        con = self.setup_connection()
        try:
            # Setup base table view
            con.execute(f"CREATE VIEW events_v AS SELECT * FROM read_parquet('{self.lake_path}/events/day=*/**/*.parquet');")
            
            # 1. Global checksums
            mv_path = Path(self.mvs_path) / mv_name
            if not mv_path.exists():
                return MVHealthCheck(
                    mv_name=mv_name,
                    status="ERROR",
                    checked_at=time.time(),
                    total_rows=0,
                    checksum_values={},
                    spot_check_ok=0,
                    spot_check_mismatched=0,
                    error_message=f"MV path not found: {mv_path}"
                )
                
            mv_pattern = f"{mv_path}/**/*.parquet" if mv_path.is_dir() else str(mv_path)
            
            # Get MV summary stats
            checksum_sql = f"""
            SELECT 
                COUNT(*) AS total_rows,
                {', '.join([f'SUM({col}) AS {col}_sum' for col in mv_def['aggregate_columns'].keys()])}
            FROM read_parquet('{mv_pattern}')
            """
            
            mv_stats = con.execute(checksum_sql).fetchone()
            if not mv_stats:
                return MVHealthCheck(
                    mv_name=mv_name,
                    status="ERROR", 
                    checked_at=time.time(),
                    total_rows=0,
                    checksum_values={},
                    spot_check_ok=0,
                    spot_check_mismatched=0,
                    error_message="Could not read MV stats"
                )
                
            total_rows = mv_stats[0]
            checksum_values = {}
            for i, col in enumerate(mv_def['aggregate_columns'].keys(), 1):
                checksum_values[f"{col}_sum"] = mv_stats[i] or 0
                
            # 2. Spot verification on random sample
            sample_size = mv_def['sample_size']
            key_cols = mv_def['key_columns']
            
            # Get random sample of keys
            sample_keys_sql = f"""
            CREATE TEMP TABLE sample_keys AS
            SELECT {', '.join(key_cols)}
            FROM read_parquet('{mv_pattern}')
            ORDER BY RANDOM()
            LIMIT {sample_size}
            """
            con.execute(sample_keys_sql)
            
            # Build verification query
            base_aggs = []
            mv_aggs = []
            
            for mv_col, base_expr in mv_def['aggregate_columns'].items():
                base_aggs.append(f"{base_expr} AS {mv_col}")
                mv_aggs.append(f"SUM({mv_col}) AS {mv_col}")
                
            # Compare base vs MV for sample keys
            verification_sql = f"""
            WITH base AS (
                SELECT {', '.join(key_cols)}, {', '.join(base_aggs)}
                FROM {mv_def['base_table']}
                WHERE ({', '.join(key_cols)}) IN (
                    SELECT {', '.join(key_cols)} FROM sample_keys
                )
                GROUP BY {', '.join(key_cols)}
            ),
            mv AS (
                SELECT {', '.join(key_cols)}, {', '.join(mv_aggs)}
                FROM read_parquet('{mv_pattern}')
                WHERE ({', '.join(key_cols)}) IN (
                    SELECT {', '.join(key_cols)} FROM sample_keys
                )
                GROUP BY {', '.join(key_cols)}
            )
            SELECT
                COUNT(*) FILTER (WHERE {' AND '.join([f'ABS(COALESCE(base.{col}, 0) - COALESCE(mv.{col}, 0)) <= 1e-6' for col in mv_def['aggregate_columns'].keys()])}) AS ok,
                COUNT(*) FILTER (WHERE {' OR '.join([f'ABS(COALESCE(base.{col}, 0) - COALESCE(mv.{col}, 0)) > 1e-6' for col in mv_def['aggregate_columns'].keys()])}) AS mismatched,
                COUNT(*) AS total_checked
            FROM base 
            FULL OUTER JOIN mv ON {' AND '.join([f'base.{col} = mv.{col}' for col in key_cols])}
            """
            
            verification_result = con.execute(verification_sql).fetchone()
            
            if verification_result:
                spot_check_ok = verification_result[0] or 0
                spot_check_mismatched = verification_result[1] or 0
                total_checked = verification_result[2] or 0
            else:
                spot_check_ok = spot_check_mismatched = total_checked = 0
                
            # Determine status
            if spot_check_mismatched > 0:
                status = "STALE"
            elif total_checked == 0:
                status = "ERROR" 
            else:
                status = "OK"
                
            return MVHealthCheck(
                mv_name=mv_name,
                status=status,
                checked_at=time.time(),
                total_rows=total_rows,
                checksum_values=checksum_values,
                spot_check_ok=spot_check_ok,
                spot_check_mismatched=spot_check_mismatched
            )
            
        except Exception as e:
            return MVHealthCheck(
                mv_name=mv_name,
                status="ERROR",
                checked_at=time.time(),
                total_rows=0,
                checksum_values={},
                spot_check_ok=0,
                spot_check_mismatched=0,
                error_message=str(e)
            )
        finally:
            con.close()
            
    def run_all_checks(self) -> Dict[str, Any]:
        """Run integrity checks on all MVs."""
        print("🔍 Starting MV Integrity Checks")
        print(f"   🏠 Lake: {self.lake_path}")
        print(f"   📊 MVs: {self.mvs_path}")
        print()
        
        mv_definitions = self.get_mv_definitions()
        
        for mv_name, mv_def in mv_definitions.items():
            try:
                health_check = self.check_mv_health(mv_name, mv_def)
                self.health_checks.append(health_check)
                
                # Print immediate feedback
                status_emoji = "✅" if health_check.status == "OK" else "⚠️" if health_check.status == "STALE" else "❌"
                print(f"   {status_emoji} {mv_name}: {health_check.status}")
                
                if health_check.status != "OK":
                    if health_check.error_message:
                        print(f"      Error: {health_check.error_message}")
                    elif health_check.spot_check_mismatched > 0:
                        print(f"      Mismatched: {health_check.spot_check_mismatched} out of {health_check.spot_check_ok + health_check.spot_check_mismatched}")
                        
            except Exception as e:
                print(f"   ❌ {mv_name}: Failed - {str(e)}")
                
        return self.generate_report()
        
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive integrity report."""
        total_mvs = len(self.health_checks)
        healthy_mvs = sum(1 for hc in self.health_checks if hc.status == "OK")
        stale_mvs = sum(1 for hc in self.health_checks if hc.status == "STALE")
        error_mvs = sum(1 for hc in self.health_checks if hc.status == "ERROR")
        
        health_rate = healthy_mvs / total_mvs if total_mvs > 0 else 0
        
        report = {
            "integrity_check": {
                "timestamp": time.time(),
                "total_mvs": total_mvs,
                "healthy_mvs": healthy_mvs,
                "stale_mvs": stale_mvs,
                "error_mvs": error_mvs,
                "health_rate": round(health_rate * 100, 1)
            },
            "mv_status": {
                hc.mv_name: {
                    "status": hc.status,
                    "total_rows": hc.total_rows,
                    "checksums": hc.checksum_values,
                    "spot_check": {
                        "ok": hc.spot_check_ok,
                        "mismatched": hc.spot_check_mismatched
                    },
                    "error": hc.error_message
                } for hc in self.health_checks
            },
            "recommendations": self._generate_recommendations(),
            "stale_mvs": [hc.mv_name for hc in self.health_checks if hc.status == "STALE"],
            "failed_mvs": [hc.mv_name for hc in self.health_checks if hc.status == "ERROR"]
        }
        
        return report
        
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on health check results."""
        recommendations = []
        
        stale_mvs = [hc for hc in self.health_checks if hc.status == "STALE"]
        error_mvs = [hc for hc in self.health_checks if hc.status == "ERROR"]
        
        if stale_mvs:
            stale_names = [hc.mv_name for hc in stale_mvs]
            recommendations.append(f"Stale MVs detected: {', '.join(stale_names)}. Rebuild required before routing queries to these MVs.")
            
        if error_mvs:
            error_names = [hc.mv_name for hc in error_mvs] 
            recommendations.append(f"MV errors detected: {', '.join(error_names)}. Investigate file paths and data consistency.")
            
        healthy_mvs = [hc for hc in self.health_checks if hc.status == "OK"]
        if len(healthy_mvs) == len(self.health_checks):
            recommendations.append("All MVs are healthy and consistent! Safe to route all queries to MVs.")
        elif len(healthy_mvs) > 0:
            recommendations.append(f"{len(healthy_mvs)} MVs are healthy. Route queries only to healthy MVs until issues are resolved.")
            
        # Check for data volume changes
        large_mvs = [hc for hc in healthy_mvs if hc.total_rows > 1000000]
        if large_mvs:
            recommendations.append(f"Large MVs detected ({len(large_mvs)} with >1M rows). Monitor query performance and consider additional partitioning.")
            
        return recommendations
        
    def update_mv_health_table(self, con: duckdb.DuckDBPyConnection):
        """Update MV health tracking table."""
        try:
            # Create health tracking table if it doesn't exist
            con.execute("""
            CREATE TABLE IF NOT EXISTS mv_health (
                mv_name VARCHAR,
                status VARCHAR,
                checked_at TIMESTAMP,
                total_rows BIGINT,
                spot_check_ok INTEGER,
                spot_check_mismatched INTEGER,
                PRIMARY KEY (mv_name, checked_at)
            )
            """)
            
            # Insert health check results
            for hc in self.health_checks:
                con.execute("""
                INSERT INTO mv_health (mv_name, status, checked_at, total_rows, spot_check_ok, spot_check_mismatched)
                VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    hc.mv_name,
                    hc.status, 
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(hc.checked_at)),
                    hc.total_rows,
                    hc.spot_check_ok,
                    hc.spot_check_mismatched
                ])
                
        except Exception as e:
            print(f"Warning: Could not update MV health table: {e}")
            
    def export_report(self, report: Dict[str, Any], output_path: str) -> str:
        """Export integrity report to JSON file."""
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        return output_path
        
    def get_stale_mvs(self) -> List[str]:
        """Get list of stale MV names for router integration."""
        return [hc.mv_name for hc in self.health_checks if hc.status == "STALE"]

def mv_is_stale(mv_name: str, health_check_path: str = "data/outputs/mv_integrity.json") -> bool:
    """Check if MV is marked as stale (for router integration)."""
    try:
        if Path(health_check_path).exists():
            with open(health_check_path, 'r') as f:
                report = json.load(f)
            return mv_name in report.get("stale_mvs", [])
    except Exception:
        pass
    return False  # Default to healthy if we can't read the status

def main():
    parser = argparse.ArgumentParser(description="MV Integrity Checker")
    parser.add_argument("--lake", required=True, help="Path to Parquet lake")
    parser.add_argument("--mvs", required=True, help="Path to materialized views")
    parser.add_argument("--out", required=True, help="Output path for integrity report")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    parser.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    parser.add_argument("--update-health-table", action="store_true", help="Update MV health tracking table")
    args = parser.parse_args()
    
    # Create integrity checker
    checker = MVIntegrityChecker(
        lake_path=args.lake,
        mvs_path=args.mvs,
        threads=args.threads,
        memory=args.mem
    )
    
    # Run integrity checks
    report = checker.run_all_checks()
    
    # Export results
    report_path = checker.export_report(report, args.out)
    
    print(f"\n🎯 MV Integrity Check Complete!")
    print(f"   📊 Report: {report_path}")
    print(f"   ✅ Health rate: {report['integrity_check']['health_rate']}%")
    print(f"   📈 Healthy MVs: {report['integrity_check']['healthy_mvs']}/{report['integrity_check']['total_mvs']}")
    
    if report['integrity_check']['stale_mvs'] > 0:
        print(f"   ⚠️  Stale MVs: {report['integrity_check']['stale_mvs']}")
        print("   🔄 Rebuild recommended before routing queries")
        
    if report['integrity_check']['error_mvs'] > 0:
        print(f"   ❌ Error MVs: {report['integrity_check']['error_mvs']}")
        print("   🔍 Investigation required")
        
    # Print recommendations
    if report["recommendations"]:
        print(f"\n💡 Recommendations:")
        for rec in report["recommendations"]:
            print(f"   • {rec}")
            
    # Update health table if requested
    if args.update_health_table:
        con = checker.setup_connection()
        try:
            checker.update_mv_health_table(con)
            print("   📝 MV health table updated")
        finally:
            con.close()

if __name__ == "__main__":
    main()