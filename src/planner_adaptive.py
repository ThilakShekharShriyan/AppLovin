#!/usr/bin/env python3
"""
Adaptive query planner with fuzzy MV matching and sampling support.

Matches queries against MVs using partial aggregation strategy:
1. Exact match - Use narrow MV
2. Partial match - Use wider MV with additional GROUP BY
3. No match - Choose between full scan or sampling based on data size
"""
from typing import Dict, Any, Tuple, List, Set, Optional


def _extract_dimensions(q: Dict[str, Any]) -> Set[str]:
    """Extract all dimension columns from group_by clause."""
    return set(q.get("group_by", []))


def _extract_filters(q: Dict[str, Any]) -> Dict[str, Any]:
    """Extract all filter columns and their constraints."""
    filters = {}
    for c in q.get("where", []):
        filters[c["col"]] = {"op": c["op"], "val": c["val"]}
    return filters


def _extract_aggregates(q: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract aggregate functions from select clause."""
    aggs = []
    for item in q.get("select", []):
        if isinstance(item, dict):
            aggs.append(item)
    return aggs


def _score_mv_match(q: Dict[str, Any], mv_info: Dict[str, Any]) -> Tuple[int, str]:
    """
    Score how well an MV matches the query.
    
    Returns (score, match_type) where:
    - score: 0-100 (higher is better)
    - match_type: "exact", "partial", "none"
    """
    q_dims = _extract_dimensions(q)
    q_filters = _extract_filters(q)
    q_aggs = _extract_aggregates(q)
    
    mv_dims = set(mv_info.get("dimensions", []))
    mv_filters = mv_info.get("filters", {})
    mv_measures = set(mv_info.get("measures", []))
    
    score = 0
    
    # Check if query dimensions are subset of MV dimensions
    if not q_dims.issubset(mv_dims):
        return (0, "none")
    
    # Check filter compatibility
    type_filter = q_filters.get("type")
    if type_filter and mv_filters.get("type"):
        if type_filter["val"] != mv_filters["type"]:
            return (0, "none")
    
    # Score based on dimension match
    if q_dims == mv_dims:
        score += 50  # Exact dimension match
        match_type = "exact"
    else:
        score += 30  # Partial match (query is coarser than MV)
        match_type = "partial"
    
    # Score based on measure availability
    for agg in q_aggs:
        for func, col in agg.items():
            if func == "SUM" and "sum_bid" in mv_measures and col == "bid_price":
                score += 20
            elif func == "COUNT" and "events" in mv_measures:
                score += 20
            elif func == "AVG" and "sum_total" in mv_measures:
                score += 15
    
    # Bonus for pre-filtered MVs
    if type_filter and mv_filters.get("type") == type_filter["val"]:
        score += 10
    
    return (score, match_type)


def choose_plan_adaptive(q: Dict[str, Any], available_mvs: List[str] = None) -> Tuple[str, Dict[str, Any]]:
    """
    Adaptive planner that chooses best execution strategy.
    
    Returns (table_name, plan_metadata) where plan_metadata includes:
    - keep_where: filters to apply
    - match_type: "exact", "partial", "sampling", "full_scan"
    - sample_rate: if using sampling
    """
    
    # Define MV metadata (dimensions, filters, measures)
    MV_CATALOG = {
        "mv_day_impr_revenue": {
            "dimensions": ["day"],
            "filters": {"type": "impression"},
            "measures": ["sum_bid", "events"]
        },
        "mv_day_country_publisher_impr": {
            "dimensions": ["day", "country", "publisher_id"],
            "filters": {"type": "impression"},
            "measures": ["sum_bid", "events"]
        },
        "mv_country_purchase_avg": {
            "dimensions": ["country"],
            "filters": {"type": "purchase"},
            "measures": ["avg_total", "n"]
        },
        "mv_adv_type_counts": {
            "dimensions": ["advertiser_id", "type"],
            "filters": {},
            "measures": ["n"]
        },
        "mv_day_minute_impr": {
            "dimensions": ["day", "minute"],
            "filters": {"type": "impression"},
            "measures": ["sum_bid", "events"]
        },
        # WIDER MVs
        "mv_hour_country_pub_adv_impr": {
            "dimensions": ["hour", "day", "country", "publisher_id", "advertiser_id"],
            "filters": {"type": "impression"},
            "measures": ["sum_bid", "events"]
        },
        "mv_day_country_type_adv": {
            "dimensions": ["day", "country", "type", "advertiser_id", "publisher_id"],
            "filters": {},
            "measures": ["events", "sum_bid", "sum_total"]
        },
        "mv_week_adv_country_type": {
            "dimensions": ["week", "advertiser_id", "country", "type"],
            "filters": {},
            "measures": ["events", "sum_bid", "sum_total", "unique_users"]
        }
    }
    
    # Filter to available MVs if specified
    if available_mvs:
        catalog = {k: v for k, v in MV_CATALOG.items() if k in available_mvs}
    else:
        catalog = MV_CATALOG
    
    # Score all MVs
    scores = []
    for mv_name, mv_info in catalog.items():
        score, match_type = _score_mv_match(q, mv_info)
        if score > 0:
            scores.append((score, match_type, mv_name, mv_info))
    
    # Sort by score (descending)
    scores.sort(reverse=True, key=lambda x: x[0])
    
    if scores:
        best_score, match_type, mv_name, mv_info = scores[0]
        
        # Determine which filters to keep
        q_filters = _extract_filters(q)
        mv_dims = mv_info["dimensions"]
        mv_filters = mv_info.get("filters", {})
        
        # Keep filters on dimensions + partitioning columns
        keep_cols = set(mv_dims) | {"day", "week", "hour"}
        
        # Don't keep type filter if MV is already filtered by type
        keep_where = []
        for col in q_filters.keys():
            if col == "type" and mv_filters.get("type"):
                # Skip type filter - MV is pre-filtered
                continue
            if col in keep_cols:
                keep_where.append(col)
        
        return (mv_name, {
            "keep_where": keep_where,
            "match_type": match_type,
            "score": best_score,
            "needs_reaggregate": match_type == "partial"
        })
    
    # No MV match - decide between full scan or sampling
    q_dims = _extract_dimensions(q)
    q_filters = _extract_filters(q)
    
    # Use sampling if:
    # 1. Query has aggregates (not raw data retrieval)
    # 2. No tight date filter (broad scan)
    # 3. Group by has high cardinality dimensions
    has_aggs = len(_extract_aggregates(q)) > 0
    has_date_filter = any(c in q_filters for c in ["day", "week", "hour"])
    high_cardinality = any(dim in q_dims for dim in ["user_id", "auction_id", "minute"])
    
    if has_aggs and not has_date_filter and high_cardinality:
        return ("events_v", {
            "keep_where": list(q_filters.keys()),
            "match_type": "sampling",
            "sample_rate": 0.1,  # 10% sample
            "approximate": True
        })
    
    # Default: full scan
    return ("events_v", {
        "keep_where": list(q_filters.keys()),
        "match_type": "full_scan",
        "approximate": False
    })


if __name__ == "__main__":
    # Test cases
    test_queries = [
        {
            "name": "Exact match - daily revenue",
            "query": {
                "from": "events",
                "select": ["day", {"SUM": "bid_price"}],
                "where": [{"col": "type", "op": "eq", "val": "impression"}],
                "group_by": ["day"]
            }
        },
        {
            "name": "Partial match - hourly revenue by advertiser (uses wide MV)",
            "query": {
                "from": "events",
                "select": ["hour", "advertiser_id", {"SUM": "bid_price"}],
                "where": [{"col": "type", "op": "eq", "val": "impression"}],
                "group_by": ["hour", "advertiser_id"]
            }
        },
        {
            "name": "No match - should use sampling",
            "query": {
                "from": "events",
                "select": ["user_id", {"COUNT": "*"}],
                "where": [],
                "group_by": ["user_id"]
            }
        }
    ]
    
    for test in test_queries:
        table, plan = choose_plan_adaptive(test["query"])
        print(f"\n{test['name']}:")
        print(f"  → Table: {table}")
        print(f"  → Match type: {plan['match_type']}")
        print(f"  → Score: {plan.get('score', 'N/A')}")
        if plan.get("sample_rate"):
            print(f"  → Sample rate: {plan['sample_rate']*100}%")
