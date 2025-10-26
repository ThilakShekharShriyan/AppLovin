#!/usr/bin/env python3
"""
MV-first query planner - eagerly routes to wide MVs for maximum speedup.
"""
from typing import Tuple, Dict, Any


def _grain(q):
    """Extract time grain from query columns."""
    cols = set(q.get("group_by", [])) | {c for c in q.get("select", []) if isinstance(c, str)}
    if "minute" in cols: return "minute"
    if "hour" in cols: return "hour" 
    if "day" in cols: return "day"
    if "week" in cols: return "week"
    return None


def _type_eq(wh, val):
    """Check if WHERE clause has type='val'."""
    return any(c["col"]=="type" and c["op"]=="eq" and c["val"]==val for c in (wh or []))


def _dims(gb):
    """Extract valid dimensions from group_by."""
    return [c for c in gb if c in ("advertiser_id", "publisher_id", "country", "type", "user_id")]


def choose_plan(q: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Choose execution plan - MV-first strategy for maximum speedup."""
    sel, gb, wh = q.get("select", []), q.get("group_by", []), q.get("where", [])
    grain = _grain(q)
    dims = _dims(gb)
    
    # Known 2-D MVs: check FIRST (more specific than 1D)
    
    # All-time counts per advertiser × type (q4 bottleneck fix)
    if set(gb) == {"advertiser_id", "type"} and any(
        isinstance(x, dict) and next(iter(x)).upper() == "COUNT" for x in sel
    ) and not wh:  # No filters - all-time counts
        return ("mv_all_adv_type_counts", {"measure": "count_all", "keep_where": []})
    
    # Publisher by country (2D with filters)
    if set(gb) == {"publisher_id"} and any(c["col"]=="country" for c in wh) and _type_eq(wh, "impression"):
        return ("mv_day_country_publisher_impr", {"measure": "impr", "keep_where": ["day", "country"]})
    
    # Minute grain: only if day is fixed (avoid huge scans)
    if grain == "minute":
        if any(c["col"]=="day" and c["op"]=="eq" for c in wh) and _type_eq(wh, "impression"):
            return ("mv_day_minute_impr", {"keep_where": ["day"]})
        return ("events_v", {"keep_where": ["day", "country", "type", "publisher_id", "advertiser_id", "user_id"]})
    
    # Single-dimension wide MVs (the big win - covers most queries)
    if grain in ("day", "hour", "week") and len(dims) == 1:
        dim = dims[0]
        
        # Impression revenue → sum_bid_impr from wide MV
        if any(isinstance(x, dict) and next(iter(x)).upper()=="SUM" and x[next(iter(x))]=="bid_price" for x in sel) and _type_eq(wh, "impression"):
            return (f"mv_{grain}_{dim}_wide", {"measure": "impr", "keep_where": [grain, dim, "country"]})
            
        # Purchase avg → sum_total_pur / cnt_total_pur from wide MV  
        if any(isinstance(x, dict) and next(iter(x)).upper()=="AVG" and x[next(iter(x))]=="total_price" for x in sel) and _type_eq(wh, "purchase"):
            return (f"mv_{grain}_{dim}_wide", {"measure": "purchase", "keep_where": [grain, dim, "country"]})
            
        # Count queries → events_all from wide MV
        if any(isinstance(x, dict) and next(iter(x)).upper()=="COUNT" for x in sel):
            return (f"mv_{grain}_{dim}_wide", {"measure": "count", "keep_where": [grain, dim, "country"]})
    
    # Pure grain queries (day/hour/week aggregations without other dimensions)
    if grain in ("day", "hour", "week") and len(dims) == 0:
        # Use type as the dimension since all queries have type filters
        # Impression revenue by day/hour/week
        if any(isinstance(x, dict) and next(iter(x)).upper()=="SUM" and x[next(iter(x))]=="bid_price" for x in sel) and _type_eq(wh, "impression"):
            return (f"mv_{grain}_type_wide", {"measure": "impr", "keep_where": [grain, "type"]})
    
    # Pure dimension queries (no time grain)
    if grain is None and len(dims) == 1:
        dim = dims[0]
        
        # Check for MIN/MAX aggregates - wide MVs don't support these
        has_min_max = any(isinstance(x, dict) and next(iter(x)).upper() in ["MIN", "MAX"] for x in sel)
        if has_min_max:
            return ("events_v", {"keep_where": ["day", dim, "type"]})
        
        # Default to day grain for pure dimension queries
        # Impression revenue by publisher/advertiser/country  
        if any(isinstance(x, dict) and next(iter(x)).upper()=="SUM" and x[next(iter(x))]=="bid_price" for x in sel) and _type_eq(wh, "impression"):
            return (f"mv_day_{dim}_wide", {"measure": "impr", "keep_where": ["day", dim, "country"]})
        # Purchase revenue by dimension (SUM total_price) - must have purchase type filter
        if any(isinstance(x, dict) and next(iter(x)).upper()=="SUM" and x[next(iter(x))]=="total_price" for x in sel) and _type_eq(wh, "purchase"):
            return (f"mv_day_{dim}_wide", {"measure": "purchase", "keep_where": ["day", dim]})
        # Cross-type queries (e.g., SUM total_price with impression filter) - fallback to events_v
        if any(isinstance(x, dict) and next(iter(x)).upper()=="SUM" and x[next(iter(x))]=="total_price" for x in sel) and _type_eq(wh, "impression"):
            return ("events_v", {"keep_where": ["day", dim, "type"]})
        # Purchase avg by country (use day grain for partitioning)
        if any(isinstance(x, dict) and next(iter(x)).upper()=="AVG" and x[next(iter(x))]=="total_price" for x in sel) and _type_eq(wh, "purchase"):
            return (f"mv_day_{dim}_wide", {"measure": "purchase", "keep_where": ["day", dim]})
        # Count by advertiser/publisher/country  
        if any(isinstance(x, dict) and next(iter(x)).upper()=="COUNT" for x in sel):
            return (f"mv_day_{dim}_wide", {"measure": "count", "keep_where": ["day", dim]})
    
    # Two-dimension queries: most need full scan since wide MVs only have 1 dimension each
    if grain is None and len(dims) == 2:
        # advertiser_id + type → no single wide MV has both, use events_v
        if "advertiser_id" in dims and "type" in dims:
            if any(isinstance(x, dict) and next(iter(x)).upper()=="COUNT" for x in sel):
                return ("events_v", {"keep_where": ["day", "advertiser_id", "type"]})
    
    # Fallback to partition-aware events_v
    return ("events_v", {"keep_where": ["day", "hour", "week", "minute", "type", "country", "publisher_id", "advertiser_id", "user_id"]})


if __name__ == "__main__":
    # Test cases
    test_queries = [
        {
            "from": "events",
            "select": ["day", {"SUM": "bid_price"}],
            "where": [{"col": "type", "op": "eq", "val": "impression"}],
            "group_by": ["day"]
        },
        {
            "from": "events",
            "select": ["country", {"AVG": "total_price"}],
            "where": [{"col": "type", "op": "eq", "val": "purchase"}],
            "group_by": ["country"]
        }
    ]
    
    for i, q in enumerate(test_queries, 1):
        table, proj = choose_plan(q)
        print(f"Query {i}: {table} (keep_where={proj['keep_where']})")
