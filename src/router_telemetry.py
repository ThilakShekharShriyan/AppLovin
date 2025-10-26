#!/usr/bin/env python3
"""
Router Telemetry System for AppLovin Query Engine

Tracks MV hit/miss rates, fallback reasons, and performance metrics.
Outputs sidecar .router.json files alongside query results.
"""

import json
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

class MVRoutingStatus(Enum):
    HIT = "hit"
    MISS = "miss" 
    PARTIAL_HIT = "partial_hit"
    FALLBACK = "fallback"

class FallbackReason(Enum):
    NO_MV_MATCH = "no_mv_match"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    MISSING_COLUMNS = "missing_columns"
    COMPLEX_JOINS = "complex_joins"
    CUSTOM_FILTER = "custom_filter"
    PARTITION_MISMATCH = "partition_mismatch"

@dataclass
class RouterMetrics:
    query_file: str
    routing_status: MVRoutingStatus
    selected_table: str
    fallback_reason: Optional[FallbackReason]
    mv_candidates_evaluated: List[str]
    routing_decision_time_ms: float
    query_execution_time_ms: float
    rows_scanned: Optional[int]
    files_scanned: Optional[int]
    total_files_available: Optional[int]
    partition_pruning_effective: bool
    memory_usage_mb: Optional[float]
    io_bytes_read: Optional[int]
    scan_duration_ms: Optional[float]
    profiling_info: Optional[str]

class RouterTelemetry:
    def __init__(self):
        self.metrics: List[RouterMetrics] = []
        self.session_start = time.time()
        
    def record_routing_decision(
        self,
        query_file: str,
        selected_table: str,
        mv_candidates: List[str],
        routing_time_ms: float,
        status: MVRoutingStatus = MVRoutingStatus.HIT,
        fallback_reason: Optional[FallbackReason] = None
    ) -> str:
        """Record a routing decision and return a tracking ID"""
        metric = RouterMetrics(
            query_file=query_file,
            routing_status=status,
            selected_table=selected_table,
            fallback_reason=fallback_reason,
            mv_candidates_evaluated=mv_candidates,
            routing_decision_time_ms=routing_time_ms,
            query_execution_time_ms=0.0,  # Will be updated later
            rows_scanned=None,
            files_scanned=None,
            total_files_available=None,
            partition_pruning_effective=False,
            memory_usage_mb=None,
            io_bytes_read=None
        )
        self.metrics.append(metric)
        return f"{query_file}_{len(self.metrics)-1}"
        
    def update_execution_metrics(
        self,
        tracking_id: str,
        execution_time_ms: float,
        rows_scanned: Optional[int] = None,
        files_scanned: Optional[int] = None,
        total_files: Optional[int] = None,
        memory_mb: Optional[float] = None,
        io_bytes: Optional[int] = None,
        scan_duration_ms: Optional[float] = None,
        profiling_info: Optional[str] = None
    ):
        """Update execution metrics for a tracked query"""
        # Extract index from tracking_id
        try:
            idx = int(tracking_id.split('_')[-1])
            if 0 <= idx < len(self.metrics):
                metric = self.metrics[idx]
                metric.query_execution_time_ms = execution_time_ms
                metric.rows_scanned = rows_scanned
                metric.files_scanned = files_scanned
                metric.total_files_available = total_files
                if files_scanned is not None and total_files is not None:
                    metric.partition_pruning_effective = files_scanned < total_files
                metric.memory_usage_mb = memory_mb
                metric.io_bytes_read = io_bytes
                metric.scan_duration_ms = scan_duration_ms
                metric.profiling_info = profiling_info
        except (ValueError, IndexError):
            pass  # Invalid tracking_id, ignore
            
    def export_sidecar(self, output_path: str, query_file: str) -> str:
        """Export telemetry as sidecar JSON file"""
        # Find metrics for this query
        query_metrics = [m for m in self.metrics if m.query_file == query_file]
        
        sidecar_path = output_path.replace('.csv', '.router.json')
        sidecar_data = {
            "query_file": query_file,
            "timestamp": time.time(),
            "session_duration_s": time.time() - self.session_start,
            "metrics": [asdict(m) for m in query_metrics],
            "summary": self._generate_summary()
        }
        
        with open(sidecar_path, 'w') as f:
            json.dump(sidecar_data, f, indent=2, default=str)
            
        return sidecar_path
        
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate high-level summary statistics"""
        if not self.metrics:
            return {}
            
        total_queries = len(self.metrics)
        mv_hits = sum(1 for m in self.metrics if m.routing_status == MVRoutingStatus.HIT)
        fallbacks = sum(1 for m in self.metrics if m.routing_status == MVRoutingStatus.FALLBACK)
        
        avg_routing_time = sum(m.routing_decision_time_ms for m in self.metrics) / total_queries
        avg_execution_time = sum(m.query_execution_time_ms for m in self.metrics) / total_queries
        
        fallback_reasons = {}
        for m in self.metrics:
            if m.fallback_reason:
                reason = m.fallback_reason.value
                fallback_reasons[reason] = fallback_reasons.get(reason, 0) + 1
                
        return {
            "total_queries": total_queries,
            "mv_hit_rate": mv_hits / total_queries,
            "fallback_rate": fallbacks / total_queries,
            "avg_routing_time_ms": avg_routing_time,
            "avg_execution_time_ms": avg_execution_time,
            "fallback_reasons": fallback_reasons,
            "partition_pruning_queries": sum(1 for m in self.metrics if m.partition_pruning_effective)
        }
        
    def export_batch_report(self, output_dir: str) -> str:
        """Export comprehensive batch telemetry report"""
        report_path = f"{output_dir}/router_telemetry.json"
        report_data = {
            "session_timestamp": self.session_start,
            "total_session_duration_s": time.time() - self.session_start,
            "summary": self._generate_summary(),
            "detailed_metrics": [asdict(m) for m in self.metrics],
            "performance_insights": self._generate_insights()
        }
        
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
            
        return report_path
        
    def _generate_insights(self) -> Dict[str, Any]:
        """Generate performance insights and recommendations"""
        if not self.metrics:
            return {}
            
        # Find slowest queries
        slowest = sorted(self.metrics, key=lambda m: m.query_execution_time_ms, reverse=True)[:3]
        
        # Find most common fallback reasons
        fallback_metrics = [m for m in self.metrics if m.fallback_reason]
        common_fallbacks = {}
        for m in fallback_metrics:
            reason = m.fallback_reason.value
            common_fallbacks[reason] = common_fallbacks.get(reason, 0) + 1
            
        # Calculate efficiency metrics
        mv_queries = [m for m in self.metrics if m.routing_status == MVRoutingStatus.HIT]
        fallback_queries = [m for m in self.metrics if m.routing_status == MVRoutingStatus.FALLBACK]
        
        mv_avg_time = sum(m.query_execution_time_ms for m in mv_queries) / len(mv_queries) if mv_queries else 0
        fallback_avg_time = sum(m.query_execution_time_ms for m in fallback_queries) / len(fallback_queries) if fallback_queries else 0
        
        return {
            "slowest_queries": [
                {
                    "query": m.query_file,
                    "table": m.selected_table,
                    "time_ms": m.query_execution_time_ms,
                    "status": m.routing_status.value
                } for m in slowest
            ],
            "common_fallback_reasons": common_fallbacks,
            "performance_comparison": {
                "mv_avg_time_ms": mv_avg_time,
                "fallback_avg_time_ms": fallback_avg_time,
                "mv_speedup_factor": fallback_avg_time / mv_avg_time if mv_avg_time > 0 else 0
            },
            "recommendations": self._generate_recommendations()
        }
        
    def _generate_recommendations(self) -> List[str]:
        """Generate optimization recommendations based on telemetry"""
        recommendations = []
        
        fallback_rate = sum(1 for m in self.metrics if m.routing_status == MVRoutingStatus.FALLBACK) / len(self.metrics)
        
        if fallback_rate > 0.3:
            recommendations.append("High fallback rate detected. Consider creating additional MVs for common query patterns.")
            
        slow_fallbacks = [m for m in self.metrics if m.routing_status == MVRoutingStatus.FALLBACK and m.query_execution_time_ms > 1000]
        if slow_fallbacks:
            recommendations.append("Detected slow fallback queries. Consider specialized MVs or query optimization.")
            
        memory_issues = [m for m in self.metrics if m.memory_usage_mb and m.memory_usage_mb > 1000]
        if memory_issues:
            recommendations.append("High memory usage detected. Consider increasing memory limits or optimizing data layout.")
            
        return recommendations

# Global telemetry instance
_telemetry_instance = None

def get_telemetry() -> RouterTelemetry:
    """Get global telemetry instance"""
    global _telemetry_instance
    if _telemetry_instance is None:
        _telemetry_instance = RouterTelemetry()
    return _telemetry_instance

def reset_telemetry():
    """Reset global telemetry instance"""
    global _telemetry_instance
    _telemetry_instance = RouterTelemetry()