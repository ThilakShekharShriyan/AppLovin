#!/usr/bin/env python3
"""
SQL Generation Utilities with Alias Normalization

Handles consistent alias resolution across batch queries, MVs, and ORDER BY clauses.
"""

from typing import Dict, List, Any, Set

# Canonical aggregate aliases mapping
AGG_ALIASES = {
    "SUM(bid_price)": {"sum_bid_impr", "sum_bid_price", "SUM(bid_price)"},
    "COUNT(*)": {"cnt", "events_all", "COUNT(*)"},
    "AVG(total_price)": {"avg_total_price", "AVG(total_price)"}, 
    "SUM(total_price)": {"sum_total_price", "sum_total_pur", "SUM(total_price)"},
    "MIN(total_price)": {"min_total_price", "MIN(total_price)"},
    "MAX(total_price)": {"max_total_price", "MAX(total_price)"},
}

def normalize_alias(col: str) -> str:
    """Normalize column/alias names to canonical form for consistent batching."""
    for canon, variants in AGG_ALIASES.items():
        if col in variants:
            return canon
    return col

def normalize_order_by(order_by: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Normalize ORDER BY clauses using canonical alias names."""
    if not order_by:
        return []
        
    normalized = []
    for o in order_by:
        col = normalize_alias(o["col"])
        normalized.append({"col": col, "dir": o.get("dir", "asc")})
    return normalized

def build_mv_projection(select_items: List[Any], table: str, mv_columns: Dict[str, str]) -> List[str]:
    """
    Build SELECT projection that maps MV columns to canonical aliases.
    
    Args:
        select_items: Original query SELECT items
        table: Table/MV name being queried
        mv_columns: Mapping of canonical names to actual MV column names
    
    Returns:
        List of SQL projection expressions
    """
    projections = []
    
    for item in select_items:
        if isinstance(item, str):
            # Direct column reference
            projections.append(item)
        else:
            # Aggregate function
            for func, col in item.items():
                if func == "as":
                    continue
                    
                canonical = f"{func.upper()}({col})"
                alias_name = item.get("as", canonical)
                
                # Map to actual MV column if available
                if canonical in mv_columns:
                    mv_col = mv_columns[canonical]
                    projections.append(f"{mv_col} AS \"{alias_name}\"")
                else:
                    # Use canonical form
                    projections.append(f"{canonical} AS \"{alias_name}\"")
                break
                
    return projections

def get_mv_column_mappings(table: str) -> Dict[str, str]:
    """Get mapping of canonical aggregate names to actual MV column names."""
    mappings = {}
    
    if table.endswith("_wide"):
        mappings.update({
            "SUM(bid_price)": "sum_bid_impr",
            "SUM(total_price)": "sum_total_pur", 
            "COUNT(*)": "events_all",
            "AVG(total_price)": "sum_total_pur / NULLIF(cnt_total_pur, 0)"
        })
    elif table == "mv_all_adv_type_counts":
        mappings.update({
            "COUNT(*)": "cnt"
        })
    elif table == "mv_day_minute_impr":
        mappings.update({
            "SUM(bid_price)": "sum_bid"
        })
    elif table == "mv_day_country_publisher_impr":
        mappings.update({
            "SUM(bid_price)": "sum_bid_impr"
        })
        
    return mappings

def resolve_batch_aliases(queries: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Resolve aliases across all queries in a batch to ensure consistency.
    
    Returns mapping of user aliases to canonical forms.
    """
    alias_resolution = {}
    
    for q in queries:
        for item in q.get("select", []):
            if isinstance(item, dict) and "as" in item:
                user_alias = item["as"]
                # Find the canonical form
                for func, col in item.items():
                    if func != "as":
                        canonical = f"{func.upper()}({col})"
                        alias_resolution[user_alias] = normalize_alias(canonical)
                        break
                        
    return alias_resolution

def build_order_by_sql(order_by: List[Dict[str, str]], alias_map: Dict[str, str] = None) -> str:
    """Build ORDER BY SQL with proper alias resolution."""
    if not order_by:
        return ""
        
    if alias_map is None:
        alias_map = {}
        
    parts = []
    for o in order_by:
        col = o["col"]
        # Resolve user alias to canonical form if needed
        if col in alias_map:
            col = alias_map[col]
        else:
            col = normalize_alias(col)
            
        direction = o.get("dir", "asc").upper()
        parts.append(f'"{col}" {direction}')
        
    return "ORDER BY " + ", ".join(parts)

def validate_aggregate_consistency(queries: List[Dict[str, Any]]) -> List[str]:
    """Validate that aggregates are used consistently across batch queries."""
    issues = []
    
    # Check for conflicting aliases pointing to different aggregates
    alias_to_agg = {}
    
    for q in queries:
        for item in q.get("select", []):
            if isinstance(item, dict) and "as" in item:
                user_alias = item["as"]
                for func, col in item.items():
                    if func != "as":
                        agg = f"{func.upper()}({col})"
                        canonical = normalize_alias(agg)
                        
                        if user_alias in alias_to_agg:
                            if alias_to_agg[user_alias] != canonical:
                                issues.append(f"Alias '{user_alias}' maps to different aggregates: {alias_to_agg[user_alias]} vs {canonical}")
                        else:
                            alias_to_agg[user_alias] = canonical
                        break
                        
    return issues

class BatchQueryBuilder:
    """Helper class for building consistent batch queries with proper alias handling."""
    
    def __init__(self, queries: List[Dict[str, Any]], table: str):
        self.queries = queries
        self.table = table
        self.alias_map = resolve_batch_aliases(queries)
        self.mv_columns = get_mv_column_mappings(table)
        
    def build_superset_query(self) -> str:
        """Build the superset query that can satisfy all queries in the batch."""
        # Union all SELECT items with canonical aliases
        all_selects = set()
        all_group_by = set()
        
        for q in self.queries:
            # Add dimensions
            for item in q.get("select", []):
                if isinstance(item, str):
                    all_selects.add(item)
                    
            # Add group by columns  
            for col in q.get("group_by", []):
                all_group_by.add(col)
                
        # Add canonical aggregates
        canonical_aggs = set()
        for q in self.queries:
            for item in q.get("select", []):
                if isinstance(item, dict):
                    for func, col in item.items():
                        if func != "as":
                            canonical = normalize_alias(f"{func.upper()}({col})")
                            canonical_aggs.add(canonical)
                            break
                            
        # Build SELECT clause
        select_parts = list(all_selects)
        
        for agg in canonical_aggs:
            if agg in self.mv_columns:
                mv_expr = self.mv_columns[agg]
                select_parts.append(f"{mv_expr} AS \"{agg}\"")
            else:
                select_parts.append(f"{agg} AS \"{agg}\"")
                
        select_sql = ", ".join(select_parts)
        
        # Build GROUP BY
        group_sql = f"GROUP BY {', '.join(all_group_by)}" if all_group_by else ""
        
        return f"SELECT {select_sql} FROM {self.table} {group_sql}".strip()
        
    def build_individual_projections(self) -> Dict[str, str]:
        """Build individual query projections from the superset result."""
        projections = {}
        
        for q in self.queries:
            query_id = id(q)  # Use object id as key
            proj_parts = []
            
            for item in q.get("select", []):
                if isinstance(item, str):
                    proj_parts.append(item)
                else:
                    # Use explicit alias if provided, otherwise canonical form
                    if "as" in item:
                        alias = item["as"]
                        proj_parts.append(f'"{alias}"')
                    else:
                        for func, col in item.items():
                            if func != "as":
                                canonical = normalize_alias(f"{func.upper()}({col})")
                                proj_parts.append(f'"{canonical}"')
                                break
                                
            projections[query_id] = ", ".join(proj_parts)
            
        return projections

class AliasNormalizer:
    """Utility class for normalizing SQL aliases across queries and MVs."""
    
    def __init__(self):
        self.canonical_mappings = AGG_ALIASES
        
    def normalize_aliases(self, sql: str) -> str:
        """Normalize aliases in SQL query text."""
        # Simple alias normalization - can be enhanced with SQL parsing
        normalized_sql = sql
        
        # Replace common aggregate variations
        replacements = {
            'as cnt': 'as "COUNT(*)"',
            'as events_all': 'as "COUNT(*)"',
            'as sum_bid_impr': 'as "SUM(bid_price)"',
            'as sum_bid_price': 'as "SUM(bid_price)"',
            'as sum_total_price': 'as "SUM(total_price)"',
            'as sum_total_pur': 'as "SUM(total_price)"',
            'as avg_total_price': 'as "AVG(total_price)"',
            'as min_total_price': 'as "MIN(total_price)"',
            'as max_total_price': 'as "MAX(total_price)"'
        }
        
        for old, new in replacements.items():
            normalized_sql = normalized_sql.replace(old, new)
            
        return normalized_sql
        
    def get_canonical_alias(self, alias: str) -> str:
        """Get canonical form of an alias."""
        return normalize_alias(alias)
