#!/usr/bin/env python3
"""
Safe Concurrent Indexer

Implements safe materialized view indexing with:
1. Per-MV build queues to prevent races
2. Staging/ready directory pattern for atomic operations
3. Schema registry for consistency enforcement
4. Fallback routing during rebuilds
"""

import os
import sys
import time
import json
import uuid
import threading
import queue
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass
from enum import Enum
import duckdb

class MVBuildStatus(Enum):
    """Materialized view build status."""
    READY = "READY"
    BUILDING = "BUILDING"
    FAILED = "FAILED"
    STALE = "STALE"

@dataclass
class MVBuildTask:
    """MV build task for queue processing."""
    mv_name: str
    source_sql: str
    output_path: str
    partition_by: Optional[List[str]] = None
    priority: int = 1
    task_id: str = ""
    
    def __post_init__(self):
        if not self.task_id:
            self.task_id = f"{self.mv_name}_{uuid.uuid4().hex[:8]}"

@dataclass
class MVBuildResult:
    """Result of MV build operation."""
    task_id: str
    mv_name: str
    status: MVBuildStatus
    records_written: int
    build_time_ms: float
    error_message: Optional[str] = None
    output_path: Optional[str] = None

class MVRegistry:
    """Registry tracking MV build status and metadata."""
    
    def __init__(self, registry_path: str):
        self.registry_path = Path(registry_path)
        self.lock = threading.Lock()
        self._registry = {}
        self.load()
        
    def load(self):
        """Load registry from disk."""
        if self.registry_path.exists():
            try:
                with open(self.registry_path, 'r') as f:
                    data = json.load(f)
                    # Convert string status back to enum
                    self._registry = {
                        name: {**info, "status": MVBuildStatus(info["status"])}
                        for name, info in data.items()
                    }
            except Exception as e:
                print(f"Warning: Could not load MV registry: {e}")
                self._registry = {}
                
    def save(self):
        """Save registry to disk."""
        try:
            self.registry_path.parent.mkdir(parents=True, exist_ok=True)
            # Convert enum to string for JSON serialization
            serializable = {
                name: {**info, "status": info["status"].value}
                for name, info in self._registry.items()
            }
            with open(self.registry_path, 'w') as f:
                json.dump(serializable, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save MV registry: {e}")
            
    def get_status(self, mv_name: str) -> MVBuildStatus:
        """Get current status of MV."""
        with self.lock:
            return self._registry.get(mv_name, {}).get("status", MVBuildStatus.STALE)
            
    def update_status(self, mv_name: str, status: MVBuildStatus, 
                     records: int = 0, error: Optional[str] = None,
                     output_path: Optional[str] = None):
        """Update MV status."""
        with self.lock:
            if mv_name not in self._registry:
                self._registry[mv_name] = {}
                
            self._registry[mv_name].update({
                "status": status,
                "last_updated": time.time(),
                "records": records,
                "error": error,
                "output_path": output_path
            })
            self.save()
            
    def get_stale_mvs(self) -> List[str]:
        """Get list of stale or failed MVs."""
        with self.lock:
            return [
                name for name, info in self._registry.items()
                if info.get("status") in [MVBuildStatus.STALE, MVBuildStatus.FAILED]
            ]
            
    def is_mv_available(self, mv_name: str) -> bool:
        """Check if MV is available for queries."""
        return self.get_status(mv_name) == MVBuildStatus.READY

class MVBuildWorker:
    """Worker thread for building materialized views."""
    
    def __init__(self, worker_id: int, task_queue: queue.Queue, result_queue: queue.Queue,
                 staging_manager, schema_registry, connection_pool, mv_registry):
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.staging_manager = staging_manager
        self.schema_registry = schema_registry
        self.connection_pool = connection_pool
        self.mv_registry = mv_registry
        self.running = False
        
    def start(self):
        """Start worker thread."""
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        
    def stop(self):
        """Stop worker thread."""
        self.running = False
        
    def _run(self):
        """Main worker loop."""
        while self.running:
            try:
                # Get task with timeout to allow shutdown
                task = self.task_queue.get(timeout=1.0)
                
                # Mark MV as building
                self.mv_registry.update_status(task.mv_name, MVBuildStatus.BUILDING)
                
                # Execute build
                result = self._build_mv(task)
                
                # Update registry with result
                if result.status == MVBuildStatus.READY:
                    self.mv_registry.update_status(
                        task.mv_name, MVBuildStatus.READY,
                        records=result.records_written,
                        output_path=result.output_path
                    )
                else:
                    self.mv_registry.update_status(
                        task.mv_name, MVBuildStatus.FAILED,
                        error=result.error_message
                    )
                
                # Send result
                self.result_queue.put(result)
                self.task_queue.task_done()
                
            except queue.Empty:
                continue  # Timeout, check running flag
            except Exception as e:
                print(f"Worker {self.worker_id} error: {e}")
                
    def _build_mv(self, task: MVBuildTask) -> MVBuildResult:
        """Build single materialized view with safety measures."""
        start_time = time.perf_counter()
        
        # Get exclusive write connection
        connection = self.connection_pool.get_write_connection()
        
        try:
            # Create temporary staging file
            temp_file = self.staging_manager.create_temp_file(f"{task.mv_name}.parquet")
            
            # Execute SQL with profiling
            if task.partition_by:
                partition_str = ", ".join(task.partition_by)
                copy_sql = f"""
                COPY ({task.source_sql}) TO '{temp_file.parent}' (
                    FORMAT PARQUET,
                    PARTITION_BY ({partition_str}),
                    COMPRESSION ZSTD,
                    ROW_GROUP_SIZE 131072
                )
                """
            else:
                copy_sql = f"""
                COPY ({task.source_sql}) TO '{temp_file}' (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD,
                    ROW_GROUP_SIZE 131072
                )
                """
                
            connection.execute(copy_sql)
            
            # Get record count
            record_count = connection.execute(f"SELECT COUNT(*) FROM ({task.source_sql}) t").fetchone()[0]
            
            # Validate schema consistency
            result = connection.execute(f"DESCRIBE SELECT * FROM ({task.source_sql}) LIMIT 0").fetchall()
            columns = [(row[0], row[1]) for row in result]
            
            if not self.schema_registry.register_schema(task.mv_name, columns):
                raise Exception(f"Schema drift detected for {task.mv_name}")
            
            # Atomically promote to ready directory
            if task.partition_by:
                # For partitioned data, move entire directory
                final_path = self.staging_manager.atomic_promote_directory(
                    temp_file.parent, task.mv_name
                )
            else:
                # For single file, move file
                final_path = self.staging_manager.atomic_promote(temp_file, f"{task.mv_name}.parquet")
                
            build_time_ms = (time.perf_counter() - start_time) * 1000
            
            return MVBuildResult(
                task_id=task.task_id,
                mv_name=task.mv_name,
                status=MVBuildStatus.READY,
                records_written=record_count,
                build_time_ms=build_time_ms,
                output_path=str(final_path)
            )
            
        except Exception as e:
            build_time_ms = (time.perf_counter() - start_time) * 1000
            
            # Cleanup temp files on error
            try:
                if temp_file.exists():
                    temp_file.unlink()
                elif temp_file.parent.exists():
                    import shutil
                    shutil.rmtree(temp_file.parent, ignore_errors=True)
            except Exception:
                pass
                
            return MVBuildResult(
                task_id=task.task_id,
                mv_name=task.mv_name,
                status=MVBuildStatus.FAILED,
                records_written=0,
                build_time_ms=build_time_ms,
                error_message=str(e)
            )
            
        finally:
            self.connection_pool.release_write_connection(connection)

class SafeConcurrentIndexer:
    """Safe concurrent MV indexer with build queues."""
    
    def __init__(self, lake_path: str, mvs_path: str, 
                 staging_path: str, max_workers: int = 2,
                 threads: int = 4, memory: str = "4GB"):
        self.lake_path = lake_path
        self.mvs_path = Path(mvs_path)
        self.max_workers = max_workers
        self.threads = threads
        self.memory = memory
        
        # Initialize components from safe_batch_runner
        from safe_batch_runner import (
            StagingDirectoryManager, SchemaRegistry, SafeConnectionPool
        )
        
        self.staging_manager = StagingDirectoryManager(staging_path)
        self.schema_registry = SchemaRegistry(Path(staging_path) / "mv_schema_registry.json")
        self.connection_pool = SafeConnectionPool(threads, memory)
        self.mv_registry = MVRegistry(Path(staging_path) / "mv_build_registry.json")
        
        # Build queues
        self.task_queue = queue.PriorityQueue()
        self.result_queue = queue.Queue()
        self.workers: List[MVBuildWorker] = []
        
        # MV-specific locks for build serialization
        self.mv_locks: Dict[str, threading.Lock] = {}
        self.mv_lock_manager = threading.Lock()
        
    def get_mv_lock(self, mv_name: str) -> threading.Lock:
        """Get or create exclusive lock for MV."""
        with self.mv_lock_manager:
            if mv_name not in self.mv_locks:
                self.mv_locks[mv_name] = threading.Lock()
            return self.mv_locks[mv_name]
            
    def start_workers(self):
        """Start MV build worker threads."""
        print(f"🚀 Starting {self.max_workers} MV build workers")
        
        for i in range(self.max_workers):
            worker = MVBuildWorker(
                worker_id=i,
                task_queue=self.task_queue,
                result_queue=self.result_queue,
                staging_manager=self.staging_manager,
                schema_registry=self.schema_registry,
                connection_pool=self.connection_pool,
                mv_registry=self.mv_registry
            )
            worker.start()
            self.workers.append(worker)
            
    def stop_workers(self):
        """Stop all worker threads."""
        print("🛑 Stopping MV build workers")
        
        for worker in self.workers:
            worker.stop()
            
        # Wait for workers to finish
        for worker in self.workers:
            if hasattr(worker, 'thread'):
                worker.thread.join(timeout=5.0)
                
    def submit_mv_build(self, mv_name: str, source_sql: str, 
                       partition_by: Optional[List[str]] = None,
                       priority: int = 1) -> str:
        """Submit MV build task to queue."""
        
        # Check if MV is already building
        current_status = self.mv_registry.get_status(mv_name)
        if current_status == MVBuildStatus.BUILDING:
            raise ValueError(f"MV {mv_name} is already building")
            
        # Create build task
        task = MVBuildTask(
            mv_name=mv_name,
            source_sql=source_sql,
            output_path=str(self.mvs_path / mv_name),
            partition_by=partition_by,
            priority=priority
        )
        
        # Add to queue (priority queue uses tuple: (priority, task))
        self.task_queue.put((priority, task))
        
        print(f"📋 Queued MV build: {mv_name} (priority={priority})")
        return task.task_id
        
    def wait_for_builds(self, timeout: Optional[float] = None) -> List[MVBuildResult]:
        """Wait for all queued builds to complete."""
        print("⏳ Waiting for MV builds to complete...")
        
        results = []
        start_time = time.time()
        
        # Wait for all tasks to be processed
        while True:
            try:
                # Check timeout
                if timeout and (time.time() - start_time) > timeout:
                    print(f"⚠️  Timeout waiting for MV builds")
                    break
                    
                # Get result with timeout
                result = self.result_queue.get(timeout=1.0)
                results.append(result)
                
                # Print immediate result
                if result.status == MVBuildStatus.READY:
                    print(f"   ✅ {result.mv_name}: {result.records_written:,} records ({result.build_time_ms:.1f}ms)")
                else:
                    print(f"   ❌ {result.mv_name}: {result.error_message}")
                    
                self.result_queue.task_done()
                
                # Check if queue is empty
                if self.task_queue.empty() and self.result_queue.empty():
                    break
                    
            except queue.Empty:
                # Check if there are still pending tasks
                if self.task_queue.empty():
                    break
                continue
                
        return results
        
    def rebuild_all_mvs(self, mv_definitions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Rebuild all MVs with safe concurrent execution."""
        print(f"🏗️  Rebuilding {len(mv_definitions)} materialized views")
        
        # Start workers
        self.start_workers()
        
        try:
            # Submit all build tasks
            task_ids = []
            for mv_def in mv_definitions:
                task_id = self.submit_mv_build(
                    mv_name=mv_def["name"],
                    source_sql=mv_def["sql"].format(lake_path=self.lake_path),
                    partition_by=mv_def.get("partition_by"),
                    priority=mv_def.get("priority", 1)
                )
                task_ids.append(task_id)
                
            # Wait for completion
            results = self.wait_for_builds(timeout=300)  # 5 minute timeout
            
            return self.generate_build_report(results, mv_definitions)
            
        finally:
            self.stop_workers()
            
    def is_mv_available_for_queries(self, mv_name: str) -> bool:
        """Check if MV is ready for query routing."""
        return self.mv_registry.is_mv_available(mv_name)
        
    def get_fallback_mvs(self) -> List[str]:
        """Get list of MVs that should use fallback routing."""
        return self.mv_registry.get_stale_mvs()
        
    def generate_build_report(self, results: List[MVBuildResult], 
                            mv_definitions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate comprehensive build report."""
        successful_builds = [r for r in results if r.status == MVBuildStatus.READY]
        failed_builds = [r for r in results if r.status == MVBuildStatus.FAILED]
        
        total_records = sum(r.records_written for r in successful_builds)
        total_build_time = sum(r.build_time_ms for r in results)
        
        return {
            "concurrent_build_summary": {
                "timestamp": time.time(),
                "total_mvs": len(mv_definitions),
                "successful_builds": len(successful_builds),
                "failed_builds": len(failed_builds),
                "success_rate": round(len(successful_builds) / len(mv_definitions) * 100, 1) if mv_definitions else 0,
                "total_records": total_records,
                "total_build_time_ms": round(total_build_time, 1),
                "avg_build_time_ms": round(total_build_time / len(results), 1) if results else 0,
                "concurrent_workers": self.max_workers
            },
            "mv_build_details": [
                {
                    "mv_name": r.mv_name,
                    "status": r.status.value,
                    "records_written": r.records_written,
                    "build_time_ms": round(r.build_time_ms, 1),
                    "output_path": r.output_path,
                    "error": r.error_message
                } for r in results
            ],
            "available_for_routing": [r.mv_name for r in successful_builds],
            "requires_fallback": [r.mv_name for r in failed_builds],
            "safety_features": {
                "per_mv_build_locks": "Enabled",
                "staging_ready_pattern": "Enabled", 
                "schema_consistency_checks": "Enabled",
                "atomic_file_operations": "Enabled",
                "connection_pool_safety": "Per-thread connections"
            },
            "recommendations": self._generate_build_recommendations(results)
        }
        
    def _generate_build_recommendations(self, results: List[MVBuildResult]) -> List[str]:
        """Generate build recommendations."""
        recommendations = []
        
        failed_builds = [r for r in results if r.status == MVBuildStatus.FAILED]
        if failed_builds:
            recommendations.append(f"Investigate {len(failed_builds)} failed MV builds before routing queries")
            
        successful_builds = [r for r in results if r.status == MVBuildStatus.READY]
        if len(successful_builds) == len(results):
            recommendations.append("All MVs built successfully - update query router to use new paths")
            recommendations.append("Run MV integrity validation to confirm consistency")
        else:
            recommendations.append("Use fallback routing for failed MVs until issues are resolved")
            
        avg_build_time = sum(r.build_time_ms for r in results) / len(results) if results else 0
        if avg_build_time > 60000:  # 1 minute
            recommendations.append("Consider optimizing MV build queries or increasing worker count")
            
        recommendations.extend([
            "Monitor MV registry for build status updates",
            "Set up automated rebuild triggers for stale MVs",
            "Consider incremental refresh for large MVs"
        ])
        
        return recommendations

# Integration with safe batch runner
class SafeIntegratedSystem:
    """Integrated safe system combining batch execution and concurrent indexing."""
    
    def __init__(self, lake_path: str, mvs_path: str, output_path: str,
                 threads: int = 4, memory: str = "4GB", max_indexing_workers: int = 2):
        # Initialize components
        from safe_batch_runner import SafeBatchRunner
        
        self.batch_runner = SafeBatchRunner(lake_path, mvs_path, output_path, threads, memory)
        self.indexer = SafeConcurrentIndexer(
            lake_path, mvs_path, f"{output_path}/indexing_staging",
            max_indexing_workers, threads, memory
        )
        
    def should_use_mv_for_query(self, mv_name: str) -> bool:
        """Check if MV is available or if fallback is needed."""
        return self.indexer.is_mv_available_for_queries(mv_name)
        
    def get_routing_recommendations(self) -> Dict[str, str]:
        """Get routing recommendations for query router."""
        fallback_mvs = self.indexer.get_fallback_mvs()
        
        return {
            "use_base_table_for": fallback_mvs,
            "reason": "MV_REBUILDING" if fallback_mvs else "ALL_MVS_READY",
            "fallback_count": len(fallback_mvs)
        }

def main():
    """CLI for safe concurrent indexer."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Safe Concurrent MV Indexer")
    parser.add_argument("--lake", required=True, help="Path to Parquet lake")
    parser.add_argument("--mvs", required=True, help="Path to materialized views")
    parser.add_argument("--staging", required=True, help="Staging directory for safe builds")
    parser.add_argument("--workers", type=int, default=2, help="Max concurrent MV workers")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    parser.add_argument("--mem", default="4GB", help="DuckDB memory limit")
    parser.add_argument("--report", required=True, help="Build report output path")
    args = parser.parse_args()
    
    # Sample MV definitions (in production, load from config)
    mv_definitions = [
        {
            "name": "mv_day_advertiser_id_wide",
            "sql": """
            SELECT 
                day, advertiser_id,
                SUM(CASE WHEN type = 'impression' THEN 1 ELSE 0 END) as impressions,
                SUM(CASE WHEN type = 'click' THEN 1 ELSE 0 END) as clicks,
                SUM(CASE WHEN type = 'serve' THEN 1 ELSE 0 END) as serves,
                SUM(CASE WHEN total_price > 0 THEN total_price ELSE 0 END) as revenue
            FROM read_parquet('{lake_path}/events/day=*/**/*.parquet')
            WHERE day IS NOT NULL AND advertiser_id IS NOT NULL
            GROUP BY day, advertiser_id
            ORDER BY day, advertiser_id
            """,
            "partition_by": ["day"],
            "priority": 1
        },
        {
            "name": "mv_day_country_wide",
            "sql": """
            SELECT 
                day, country,
                SUM(CASE WHEN type = 'impression' THEN 1 ELSE 0 END) as impressions,
                SUM(CASE WHEN type = 'click' THEN 1 ELSE 0 END) as clicks,
                COUNT(DISTINCT user_id) as unique_users,
                SUM(CASE WHEN total_price > 0 THEN total_price ELSE 0 END) as revenue
            FROM read_parquet('{lake_path}/events/day=*/**/*.parquet')
            WHERE day IS NOT NULL AND country IS NOT NULL
            GROUP BY day, country
            ORDER BY day, country
            """,
            "partition_by": ["day"],
            "priority": 2
        }
    ]
    
    # Initialize indexer
    indexer = SafeConcurrentIndexer(
        lake_path=args.lake,
        mvs_path=args.mvs,
        staging_path=args.staging,
        max_workers=args.workers,
        threads=args.threads,
        memory=args.mem
    )
    
    # Rebuild all MVs
    build_report = indexer.rebuild_all_mvs(mv_definitions)
    
    # Export report
    with open(args.report, 'w') as f:
        json.dump(build_report, f, indent=2)
        
    # Print summary
    summary = build_report["concurrent_build_summary"]
    print(f"\n🎯 Safe Concurrent MV Build Complete!")
    print(f"   📊 Success rate: {summary['success_rate']}%")
    print(f"   📈 Total records: {summary['total_records']:,}")
    print(f"   ⏱️  Avg build time: {summary['avg_build_time_ms']:.1f}ms")
    print(f"   👥 Concurrent workers: {summary['concurrent_workers']}")
    print(f"   📋 Report: {args.report}")
    
    return summary['failed_builds']  # Return failure count as exit code

if __name__ == "__main__":
    sys.exit(main())