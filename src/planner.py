#!/usr/bin/env python3
"""
JSON query planner - determines the best MV to use for a given query.
"""
from typing import Optional, Tuple, Dict, Any, List, Set


def _has_agg(select, func, col=None):
    """Check if select list contains a specific aggregate function."""
    for item in select:
        if isinstance(item, dict):
            for k, v in item.items():
                if k.upper() == func.upper() and (col is None or v == col):
                    return True
    return False


def _eq_val(where, col):
    """Extract the value from an equality filter on a column."""
    for c in where or []:
        if c["col"] == col and c["op"] == "eq":
            return c["val"]
    return None


def _has_between(where, col):
    """Check if where clause has a between filter on a column."""
    for c in where or []:
        if c["col"] == col and c["op"] == "between":
            return True
    return False


def _has_eq(where, col):
    """Check if where clause has an equality filter on a column."""
    for c in where or []:
        if c["col"] == col and c["op"] == "eq":
            return True
    return False


def choose_plan(q: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Return (table_name, projection) where projection tells runner how to build SQL.
    
    Projection contains:
      - keep_where: list of column names whose filters should be preserved
    """
    sel = q.get("select", [])
    gb = set(q.get("group_by", []))
    wh = q.get("where", [])

    # Pattern 1: Daily revenue from impressions
    # SELECT day, SUM(bid_price) FROM events WHERE type='impression' GROUP BY day
    if gb == {"day"} and _has_agg(sel, "SUM", "bid_price") and _eq_val(wh, "type") == "impression":
        return ("mv_day_impr_revenue", {"keep_where": ["day"]})

    # Pattern 2: Publisher revenue in country/day (impressions)
    # SELECT publisher_id, SUM(bid_price) FROM events WHERE type='impression' AND country='US' AND day BETWEEN ... GROUP BY publisher_id
    if gb == {"publisher_id"} and _has_agg(sel, "SUM", "bid_price") and _eq_val(wh, "type") == "impression":
        if (_has_eq(wh, "country") and (_has_between(wh, "day") or _has_eq(wh, "day"))):
            return ("mv_day_country_publisher_impr", {"keep_where": ["day", "country"]})

    # Pattern 3: Average purchase by country (all time)
    # SELECT country, AVG(total_price) FROM events WHERE type='purchase' GROUP BY country
    if gb == {"country"} and _has_agg(sel, "AVG", "total_price") and _eq_val(wh, "type") == "purchase":
        return ("mv_country_purchase_avg", {"keep_where": []})  # MV already filtered to purchase

    # Pattern 4: Event counts per advertiser per type (all time)
    # SELECT advertiser_id, type, COUNT(*) FROM events GROUP BY advertiser_id, type
    if gb == {"advertiser_id", "type"} and _has_agg(sel, "COUNT"):
        return ("mv_adv_type_counts", {"keep_where": []})

    # Pattern 5: Minute breakdown on a day (impressions)
    # SELECT minute, SUM(bid_price) FROM events WHERE type='impression' AND day='2025-01-15' GROUP BY minute
    if gb == {"minute"} and _has_agg(sel, "SUM", "bid_price") and _eq_val(wh, "type") == "impression" and _has_eq(wh, "day"):
        return ("mv_day_minute_impr", {"keep_where": ["day"]})

    # Fallback: full scan on events view
    return ("events_v", {"keep_where": ["day", "country", "type", "publisher_id", "advertiser_id", "minute", "hour", "week"]})


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
