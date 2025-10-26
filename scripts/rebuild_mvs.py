#!/usr/bin/env python3
"""
Materialized View Rebuilder

Rebuilds all materialized views using the cleaned data lake with:
- Normalized date partitions (YYYY-MM-DD format)
- Validated data quality (no NULLs, proper constraints)
- Consistent SQL generation using alias normalization
"""

import os
import sys
import time
import json
from pathlib import Path
from typing import Dict, List, Any

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

import duckdb
from sqlgen import AliasNormalizer

class MVRebuilder:
    def __init__(self, lake_path: str, mv_path: str, threads: int = 4, memory: str = "4GB"):
        self.lake_path = lake_path
        self.mv_path = mv_path  
        self.threads = threads
        self.memory = memory
        self.normalizer = AliasNormalizer()
        self.results = []
        
    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Create optimized DuckDB connection."""
        con = duckdb.connect(database=":memory:")
        con.execute(f"PRAGMA threads={self.threads};")
        con.execute(f"SET memory_limit='{self.memory}';")
        con.execute("SET TimeZone='UTC';")
        return con
        
    def get_mv_definitions(self) -> List[Dict[str, Any]]:
        """Define materialized view rebuild queries."""
        return [
            {
                "name": "mv_all_adv_type_counts",
                "sql": """
                SELECT 
                    advertiser_id,
                    type,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT day) as active_days,
                    MIN(day) as first_seen,
                    MAX(day) as last_seen,
                    SUM(CASE WHEN total_price > 0 THEN total_price ELSE 0 END) as total_revenue
                FROM read_parquet('{lake_path}/events/day=*/**/*.parquet')
                WHERE advertiser_id IS NOT NULL 
                  AND type IS NOT NULL
                GROUP BY advertiser_id, type
                ORDER BY advertiser_id, type
                """,
                "format": "single_file"
            },
            {
                "name": "mv_day_advertiser_id_wide", 
                "sql": """
                SELECT 
                    day,
                    advertiser_id,
                    SUM(CASE WHEN type = 'impression' THEN 1 ELSE 0 END) as impressions,
                    SUM(CASE WHEN type = 'click' THEN 1 ELSE 0 END) as clicks,
                    SUM(CASE WHEN type = 'serve' THEN 1 ELSE 0 END) as serves,
                    SUM(CASE WHEN type = 'purchase' THEN 1 ELSE 0 END) as purchases,
                    SUM(CASE WHEN total_price > 0 THEN total_price ELSE 0 END) as revenue
                FROM read_parquet('{lake_path}/events/day=*/**/*.parquet')
                WHERE day IS NOT NULL AND advertiser_id IS NOT NULL
                GROUP BY day, advertiser_id
                ORDER BY day, advertiser_id
                """,
                "format": "partitioned",
                "partition_by": ["day"]
            },
            {
                "name": "mv_day_country_wide",
                "sql": """
                SELECT 
                    day,
                    country,
                    SUM(CASE WHEN type = 'impression' THEN 1 ELSE 0 END) as impressions,
                    SUM(CASE WHEN type = 'click' THEN 1 ELSE 0 END) as clicks,
                    SUM(CASE WHEN type = 'serve' THEN 1 ELSE 0 END) as serves,
                    SUM(CASE WHEN type = 'purchase' THEN 1 ELSE 0 END) as purchases,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN total_price > 0 THEN total_price ELSE 0 END) as revenue
                FROM read_parquet('{lake_path}/events/day=*/**/*.parquet')
                WHERE day IS NOT NULL AND country IS NOT NULL
                GROUP BY day, country
                ORDER BY day, country
                """,
                "format": "partitioned",
                "partition_by": ["day"]
            },
            {
                "name": "mv_day_type_wide",
                "sql": """
                SELECT 
                    day,
                    type,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT advertiser_id) as unique_advertisers,
                    COUNT(DISTINCT publisher_id) as unique_publishers,
                    COUNT(DISTINCT country) as unique_countries,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN total_price > 0 THEN total_price ELSE 0 END) as revenue
                FROM read_parquet('{lake_path}/events/day=*/**/*.parquet')
                WHERE day IS NOT NULL AND type IS NOT NULL
                GROUP BY day, type
                ORDER BY day, type
                """,
                "format": "partitioned", 
                "partition_by": ["day"]
            },
            {
                "name": "mv_hour_advertiser_id_wide",
                "sql": """
                SELECT 
                    DATE_TRUNC('hour', CAST(day || ' ' || LPAD(CAST(minute AS VARCHAR), 2, '0') || ':00:00' AS TIMESTAMP)) as hour,
                    day,
                    advertiser_id,
                    SUM(CASE WHEN type = 'impression' THEN 1 ELSE 0 END) as impressions,
                    SUM(CASE WHEN type = 'click' THEN 1 ELSE 0 END) as clicks,
                    SUM(CASE WHEN type = 'serve' THEN 1 ELSE 0 END) as serves,
                    COUNT(*) as total_events,
                    SUM(CASE WHEN total_price > 0 THEN total_price ELSE 0 END) as revenue
                FROM read_parquet('{lake_path}/events/day=*/**/*.parquet')
                WHERE day IS NOT NULL AND advertiser_id IS NOT NULL AND minute IS NOT NULL
                GROUP BY DATE_TRUNC('hour', CAST(day || ' ' || LPAD(CAST(minute AS VARCHAR), 2, '0') || ':00:00' AS TIMESTAMP)), day, advertiser_id
                ORDER BY hour, advertiser_id
                """,
                "format": "partitioned",
                "partition_by": ["day"]
            }
        ]
        
    def rebuild_mv(self, mv_def: Dict[str, Any]) -> Dict[str, Any]:
        """Rebuild a single materialized view."""
        mv_name = mv_def["name"]
        print(f"🔄 Rebuilding {mv_name}...")
        
        start_time = time.time()
        con = self.setup_connection()
        
        try:
            # Format SQL with actual lake path
            sql = mv_def["sql"].format(lake_path=self.lake_path)
            
            # Normalize aliases for consistency
            sql = self.normalizer.normalize_aliases(sql)
            
            # Create output directory
            output_path = Path(self.mv_path) / mv_name
            if output_path.exists():
                import shutil
                shutil.rmtree(output_path)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Execute rebuild based on format
            if mv_def["format"] == "single_file":
                # Single parquet file
                con.execute(f"""
                COPY ({sql}) TO '{output_path}/{mv_name}.parquet' (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD,
                    ROW_GROUP_SIZE 131072
                )
                """)
                
                # Get record count
                record_count = con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0]
                
            else:
                # Partitioned format
                partition_cols = mv_def.get("partition_by", ["day"])
                partition_str = ", ".join(partition_cols)
                
                con.execute(f"""
                COPY ({sql}) TO '{output_path}' (
                    FORMAT PARQUET,
                    PARTITION_BY ({partition_str}),
                    COMPRESSION ZSTD,
                    ROW_GROUP_SIZE 131072
                )
                """)
                
                # Get record count  
                record_count = con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0]
                
            duration = time.time() - start_time
            
            return {
                "mv_name": mv_name,
                "success": True,
                "record_count": record_count,
                "duration_sec": round(duration, 2),
                "output_path": str(output_path),
                "format": mv_def["format"],
                "details": f"Successfully rebuilt with {record_count:,} records in {duration:.1f}s"
            }
            
        except Exception as e:
            duration = time.time() - start_time
            return {
                "mv_name": mv_name,
                "success": False,
                "record_count": 0,
                "duration_sec": round(duration, 2),
                "output_path": str(output_path),
                "format": mv_def["format"],
                "details": f"Failed: {str(e)}"
            }
        finally:
            con.close()
            
    def rebuild_all_mvs(self) -> Dict[str, Any]:
        """Rebuild all materialized views."""
        print("🏗️  Starting Materialized View Rebuild")
        print(f"   📁 Source: {self.lake_path}")
        print(f"   📁 Output: {self.mv_path}")
        print()
        
        mv_definitions = self.get_mv_definitions()
        
        for mv_def in mv_definitions:
            result = self.rebuild_mv(mv_def)
            self.results.append(result)
            
            if result["success"]:
                print(f"   ✅ {result['mv_name']}: {result['record_count']:,} records ({result['duration_sec']}s)")
            else:
                print(f"   ❌ {result['mv_name']}: {result['details']}")
                
        return self.generate_rebuild_report()
        
    def generate_rebuild_report(self) -> Dict[str, Any]:
        """Generate comprehensive rebuild report."""
        successful_rebuilds = [r for r in self.results if r["success"]]
        failed_rebuilds = [r for r in self.results if not r["success"]]
        
        total_records = sum(r["record_count"] for r in successful_rebuilds)
        total_duration = sum(r["duration_sec"] for r in self.results)
        
        report = {
            "rebuild_summary": {
                "timestamp": time.time(),
                "total_mvs": len(self.results),
                "successful_rebuilds": len(successful_rebuilds),
                "failed_rebuilds": len(failed_rebuilds),
                "success_rate": round(len(successful_rebuilds) / len(self.results) * 100, 1) if self.results else 0,
                "total_records": total_records,
                "total_duration_sec": round(total_duration, 2),
                "avg_rebuild_time_sec": round(total_duration / len(self.results), 2) if self.results else 0
            },
            "mv_details": self.results,
            "data_source": {
                "lake_path": self.lake_path,
                "mv_output_path": self.mv_path,
                "data_quality": "Clean (post-remediation)",
                "partition_format": "Normalized YYYY-MM-DD"
            },
            "next_steps": [
                "Run MV integrity checker to validate consistency",
                "Update query router to use rebuilt materialized views",
                "Monitor MV freshness and performance",
                "Set up automated MV refresh pipeline"
            ]
        }
        
        return report
        
    def export_report(self, report: Dict[str, Any], output_path: str) -> str:
        """Export rebuild report to JSON."""
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        return output_path

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Materialized View Rebuilder")
    parser.add_argument("--lake", required=True, help="Path to cleaned data lake")
    parser.add_argument("--mvs", required=True, help="Path to materialized views output")
    parser.add_argument("--report", required=True, help="Path to rebuild report")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    parser.add_argument("--mem", default="4GB", help="DuckDB memory limit")
    args = parser.parse_args()
    
    rebuilder = MVRebuilder(
        lake_path=args.lake,
        mv_path=args.mvs,
        threads=args.threads,
        memory=args.mem
    )
    
    # Rebuild all MVs
    report = rebuilder.rebuild_all_mvs()
    
    # Export report
    report_path = rebuilder.export_report(report, args.report)
    
    print(f"\n🎯 Materialized View Rebuild Complete!")
    print(f"   📊 Report: {report_path}")
    print(f"   ✅ Success rate: {report['rebuild_summary']['success_rate']}%")
    print(f"   📈 Total records: {report['rebuild_summary']['total_records']:,}")
    print(f"   ⏱️  Total time: {report['rebuild_summary']['total_duration_sec']}s")
    
    if report['rebuild_summary']['failed_rebuilds'] > 0:
        print(f"   ⚠️  Failed rebuilds: {report['rebuild_summary']['failed_rebuilds']}")

if __name__ == "__main__":
    main()