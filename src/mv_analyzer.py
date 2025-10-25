#!/usr/bin/env python3
"""
Dynamic MV analyzer - tracks query patterns and suggests optimal MVs.

Analyzes query workload to:
1. Identify frequently requested dimension combinations
2. Suggest new MVs that would improve hit rate
3. Generate DDL for recommended MVs
"""
import json
from typing import Dict, Any, List, Set, Tuple
from collections import Counter, defaultdict
from pathlib import Path


class MVAnalyzer:
    """Analyzes query patterns and suggests optimal MVs."""
    
    def __init__(self):
        self.query_history = []
        self.dimension_combos = Counter()
        self.filter_patterns = Counter()
        self.aggregate_patterns = Counter()
        
    def record_query(self, query: Dict[str, Any], hit_mv: str = None, exec_time: float = 0):
        """Record a query execution for pattern analysis."""
        self.query_history.append({
            "query": query,
            "hit_mv": hit_mv,
            "exec_time": exec_time,
            "dimensions": tuple(sorted(query.get("group_by", []))),
            "filters": self._extract_filter_signature(query),
            "aggregates": self._extract_agg_signature(query)
        })
        
        # Update counters
        dims = tuple(sorted(query.get("group_by", [])))
        self.dimension_combos[dims] += 1
        
        filters = self._extract_filter_signature(query)
        self.filter_patterns[filters] += 1
        
        aggs = self._extract_agg_signature(query)
        self.aggregate_patterns[aggs] += 1
    
    def _extract_filter_signature(self, query: Dict[str, Any]) -> Tuple[str, ...]:
        """Extract normalized filter signature."""
        filters = []
        for f in query.get("where", []):
            # Normalize to (column, operator) without values
            filters.append(f"{f['col']}:{f['op']}")
        return tuple(sorted(filters))
    
    def _extract_agg_signature(self, query: Dict[str, Any]) -> Tuple[str, ...]:
        """Extract aggregate function signature."""
        aggs = []
        for item in query.get("select", []):
            if isinstance(item, dict):
                for func, col in item.items():
                    aggs.append(f"{func}:{col}")
        return tuple(sorted(aggs))
    
    def suggest_mvs(self, min_frequency: int = 2, max_suggestions: int = 5) -> List[Dict[str, Any]]:
        """
        Suggest new MVs based on query patterns.
        
        Args:
            min_frequency: Minimum times a pattern must appear
            max_suggestions: Maximum number of MVs to suggest
            
        Returns:
            List of MV suggestions with metadata
        """
        suggestions = []
        
        # Find dimension combinations that are frequent but not covered by existing MVs
        existing_mv_dims = {
            ("day",),
            ("day", "country", "publisher_id"),
            ("country",),
            ("advertiser_id", "type"),
            ("day", "minute"),
            ("advertiser_id", "country", "day", "hour", "publisher_id"),
            ("advertiser_id", "country", "day", "publisher_id", "type"),
            ("advertiser_id", "country", "type", "week")
        }
        
        for dims, count in self.dimension_combos.most_common():
            if count < min_frequency:
                continue
            
            if dims not in existing_mv_dims and len(dims) > 0:
                # Find common filters for this dimension combo
                common_filters = self._find_common_filters_for_dims(dims)
                common_aggs = self._find_common_aggs_for_dims(dims)
                
                suggestions.append({
                    "dimensions": list(dims),
                    "frequency": count,
                    "common_filters": common_filters,
                    "common_aggregates": common_aggs,
                    "estimated_benefit": self._estimate_benefit(dims, count)
                })
                
                if len(suggestions) >= max_suggestions:
                    break
        
        return suggestions
    
    def _find_common_filters_for_dims(self, dims: Tuple[str, ...]) -> Dict[str, str]:
        """Find most common filter values for a dimension combination."""
        filters = defaultdict(Counter)
        
        for record in self.query_history:
            if record["dimensions"] == dims:
                for f in record["query"].get("where", []):
                    col = f["col"]
                    val = f.get("val")
                    if isinstance(val, (str, int, float)):  # Skip complex values
                        filters[col][val] += 1
        
        # Return most common filter value per column
        common = {}
        for col, counter in filters.items():
            if counter:
                most_common_val, count = counter.most_common(1)[0]
                # Only include if it appears in >50% of queries
                if count / self.dimension_combos[dims] > 0.5:
                    common[col] = most_common_val
        
        return common
    
    def _find_common_aggs_for_dims(self, dims: Tuple[str, ...]) -> List[str]:
        """Find common aggregate functions for a dimension combination."""
        aggs = Counter()
        
        for record in self.query_history:
            if record["dimensions"] == dims:
                for agg in record["aggregates"]:
                    aggs[agg] += 1
        
        # Return aggregates that appear in >30% of queries
        threshold = self.dimension_combos[dims] * 0.3
        return [agg for agg, count in aggs.items() if count >= threshold]
    
    def _estimate_benefit(self, dims: Tuple[str, ...], frequency: int) -> float:
        """Estimate performance benefit of creating this MV (0-100)."""
        # Base score from frequency
        score = min(frequency * 10, 50)
        
        # Bonus for having multiple dimensions (more selective)
        score += len(dims) * 5
        
        # Bonus if queries using these dims are currently slow
        avg_time = self._avg_exec_time_for_dims(dims)
        if avg_time > 1.0:  # Slower than 1 second
            score += 20
        elif avg_time > 0.1:
            score += 10
        
        return min(score, 100)
    
    def _avg_exec_time_for_dims(self, dims: Tuple[str, ...]) -> float:
        """Calculate average execution time for queries with these dimensions."""
        times = [r["exec_time"] for r in self.query_history if r["dimensions"] == dims]
        return sum(times) / len(times) if times else 0
    
    def generate_mv_ddl(self, suggestion: Dict[str, Any], lake_path: str, mvs_path: str) -> str:
        """Generate DDL to create the suggested MV."""
        dims = suggestion["dimensions"]
        filters = suggestion.get("common_filters", {})
        aggs = suggestion.get("common_aggregates", [])
        
        # Generate MV name
        mv_name = "mv_" + "_".join(dims[:3])  # Limit name length
        if filters.get("type"):
            mv_name += f"_{filters['type']}"
        
        # Build SELECT clause
        dim_cols = ", ".join(dims)
        
        # Determine measures based on common aggregates
        measures = []
        if any("SUM:bid_price" in a for a in aggs):
            measures.append("SUM(bid_price) AS sum_bid")
        if any("SUM:total_price" in a for a in aggs):
            measures.append("SUM(total_price) AS sum_total")
        if any("COUNT" in a for a in aggs):
            measures.append("COUNT(*) AS events")
        if any("AVG" in a for a in aggs):
            measures.append("SUM(total_price) AS sum_total")  # For computing AVG later
            measures.append("COUNT(*) AS n")
        
        if not measures:
            measures = ["COUNT(*) AS events", "SUM(bid_price) AS sum_bid"]
        
        measure_cols = ", ".join(measures)
        
        # Build WHERE clause
        where_clause = ""
        if filters:
            conditions = [f"{col}='{val}'" for col, val in filters.items()]
            where_clause = "WHERE " + " AND ".join(conditions)
        
        # Determine partition column
        partition_col = next((d for d in ["day", "week", "hour"] if d in dims), dims[0])
        
        ddl = f"""
-- Suggested MV: {mv_name}
-- Frequency: {suggestion['frequency']} queries
-- Estimated benefit: {suggestion['estimated_benefit']:.0f}/100

CREATE OR REPLACE TABLE {mv_name} AS
SELECT 
    {dim_cols},
    {measure_cols}
FROM read_parquet('{lake_path}/events/day=*/**/*.parquet')
{where_clause}
GROUP BY ALL;

COPY (SELECT * FROM {mv_name})
TO '{mvs_path}/{mv_name}'
(FORMAT PARQUET, PARTITION_BY ({partition_col}), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
"""
        return ddl.strip()
    
    def generate_report(self, output_path: str):
        """Generate analysis report with MV suggestions."""
        suggestions = self.suggest_mvs()
        
        report = {
            "query_count": len(self.query_history),
            "unique_dimension_combos": len(self.dimension_combos),
            "mv_hit_rate": self._calculate_hit_rate(),
            "suggestions": suggestions,
            "top_dimension_combos": [
                {"dimensions": list(dims), "count": count}
                for dims, count in self.dimension_combos.most_common(10)
            ]
        }
        
        Path(output_path).write_text(json.dumps(report, indent=2))
        return report
    
    def _calculate_hit_rate(self) -> float:
        """Calculate percentage of queries that hit an MV."""
        if not self.query_history:
            return 0.0
        
        hits = sum(1 for r in self.query_history if r["hit_mv"] and r["hit_mv"] != "events_v")
        return (hits / len(self.query_history)) * 100


if __name__ == "__main__":
    # Example usage
    analyzer = MVAnalyzer()
    
    # Simulate query workload
    queries = [
        {"from": "events", "select": ["hour", "advertiser_id", {"SUM": "bid_price"}], 
         "where": [{"col": "type", "op": "eq", "val": "impression"}], "group_by": ["hour", "advertiser_id"]},
        {"from": "events", "select": ["hour", "advertiser_id", {"SUM": "bid_price"}], 
         "where": [{"col": "type", "op": "eq", "val": "impression"}], "group_by": ["hour", "advertiser_id"]},
        {"from": "events", "select": ["week", "country", {"COUNT": "*"}], 
         "where": [], "group_by": ["week", "country"]},
        {"from": "events", "select": ["week", "country", {"COUNT": "*"}], 
         "where": [], "group_by": ["week", "country"]},
        {"from": "events", "select": ["week", "country", {"COUNT": "*"}], 
         "where": [], "group_by": ["week", "country"]},
    ]
    
    for q in queries:
        analyzer.record_query(q, hit_mv="events_v", exec_time=2.5)
    
    # Get suggestions
    suggestions = analyzer.suggest_mvs()
    
    print("=== MV Suggestions ===\n")
    for i, sug in enumerate(suggestions, 1):
        print(f"{i}. Dimensions: {sug['dimensions']}")
        print(f"   Frequency: {sug['frequency']} queries")
        print(f"   Benefit: {sug['estimated_benefit']:.0f}/100")
        print(f"\n{analyzer.generate_mv_ddl(sug, 'data/lake', 'data/mvs')}\n")
        print("-" * 60)
