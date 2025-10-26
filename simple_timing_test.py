#!/usr/bin/env python3
"""
Simple Query Timing Test

Tests individual queries with detailed timing without complex batch processing.
"""

import json
import time
import sys
import duckdb
from pathlib import Path
from typing import Dict, List, Any

def setup_duckdb_connection(memory: str = "12GB"):
    """Setup DuckDB connection with proper configuration."""
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{memory}';")
    con.execute("SET threads=8;")
    return con

def execute_query_with_timing(con: duckdb.DuckDBPyConnection, query: Dict[str, Any], query_name: str) -> Dict[str, Any]:
    """Execute a single query with timing."""
    
    print(f"🔍 Testing {query_name}")
    
    # Simple SQL builder for basic queries
    select_parts = []
    for item in query.get("select", []):
        if isinstance(item, str):
            select_parts.append(item)
        elif isinstance(item, dict):
            for func, col in item.items():
                if func.upper() in ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX']:
                    if col == "*":
                        select_parts.append(f"{func.upper()}(*)")
                    else:
                        select_parts.append(f"{func.upper()}({col})")
                    break
    
    select_sql = ", ".join(select_parts)
    
    # Build WHERE clause
    where_parts = []
    for clause in query.get("where", []):
        col = clause["col"]
        op = clause["op"]
        val = clause["val"]
        
        if op == "eq":
            where_parts.append(f"{col} = '{val}'")
        elif op == "in":
            val_str = "', '".join(str(v) for v in val)
            where_parts.append(f"{col} IN ('{val_str}')")
        elif op == "between":
            where_parts.append(f"{col} BETWEEN '{val[0]}' AND '{val[1]}'")
        elif op == "neq":
            where_parts.append(f"{col} != '{val}'")
    
    where_sql = " AND ".join(where_parts) if where_parts else ""
    
    # Build GROUP BY
    group_sql = ", ".join(query.get("group_by", []))
    
    # Build ORDER BY
    order_parts = []
    for order in query.get("order_by", []):
        col = order["col"]
        direction = order.get("dir", "asc").upper()
        order_parts.append(f"{col} {direction}")
    order_sql = ", ".join(order_parts)
    
    # Build LIMIT
    limit_sql = f"LIMIT {query.get('limit', '')}" if query.get('limit') else ""
    
    # First try MVs, then fallback to base table
    tables_to_try = [
        "data/mvs_rebuilt/mv_day_advertiser_id_wide.parquet",
        "data/mvs_rebuilt/mv_day_country_wide.parquet", 
        "data/mvs_rebuilt/mv_day_type_wide.parquet",
        "data/lake/**/*.parquet"
    ]
    
    best_result = None
    best_time = float('inf')
    table_used = None
    
    for table in tables_to_try:
        try:
            # Build full query
            sql_parts = [f"SELECT {select_sql}"]
            sql_parts.append(f"FROM read_parquet('{table}')")
            
            if where_sql:
                sql_parts.append(f"WHERE {where_sql}")
            if group_sql:
                sql_parts.append(f"GROUP BY {group_sql}")
            if order_sql:
                sql_parts.append(f"ORDER BY {order_sql}")
            if limit_sql:
                sql_parts.append(limit_sql)
                
            sql = " ".join(sql_parts)
            
            # Time the execution
            start_time = time.perf_counter()
            result = con.execute(sql).fetchall()
            execution_time = (time.perf_counter() - start_time) * 1000
            
            if execution_time < best_time:
                best_time = execution_time
                best_result = result
                table_used = table.split('/')[-1] if '/' in table else table
                
            print(f"   ⚡ {table_used}: {execution_time:.1f}ms ({len(result)} rows)")
            
            # If this is fast enough, use it
            if execution_time < 100:  # Under 100ms is good
                break
                
        except Exception as e:
            print(f"   ❌ {table}: Failed - {str(e)[:50]}")
            continue
    
    if best_result is not None:
        return {
            "query_name": query_name,
            "success": True,
            "execution_time_ms": round(best_time, 2),
            "table_used": table_used,
            "row_count": len(best_result),
            "rows_sample": best_result[:3] if best_result else []
        }
    else:
        return {
            "query_name": query_name,
            "success": False,
            "execution_time_ms": 0,
            "table_used": None,
            "row_count": 0,
            "error": "No table worked"
        }

def main():
    """Run simple timing tests on consolidated queries."""
    
    print("🚀 Simple Query Timing Test")
    print("=" * 50)
    
    # Setup connection
    con = setup_duckdb_connection("12GB")
    print("✅ DuckDB connection established")
    
    # Load queries
    queries_dir = Path("queries/consolidated_individual")
    query_files = sorted(queries_dir.glob("*.json"))
    
    if not query_files:
        print("❌ No query files found")
        return
        
    print(f"📋 Found {len(query_files)} query files")
    print()
    
    results = []
    
    for query_file in query_files:
        with open(query_file, 'r') as f:
            query = json.load(f)
        
        result = execute_query_with_timing(con, query, query_file.stem)
        results.append(result)
        print()
    
    # Print summary
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    
    print("📊 TIMING SUMMARY")
    print("=" * 30)
    print(f"Successful: {len(successful)}/{len(results)}")
    print(f"Failed: {len(failed)}")
    
    if successful:
        times = [r["execution_time_ms"] for r in successful]
        rows = [r["row_count"] for r in successful]
        
        print(f"\n⚡ Performance:")
        print(f"  Average: {sum(times)/len(times):.1f}ms")
        print(f"  Min: {min(times):.1f}ms")
        print(f"  Max: {max(times):.1f}ms")
        print(f"  Total rows: {sum(rows):,}")
        
        print(f"\n🎯 Detailed Results:")
        for result in successful:
            print(f"  {result['query_name']}: {result['execution_time_ms']:.1f}ms ({result['row_count']} rows) - {result['table_used']}")
    
    # Save results
    output_file = Path("reports/simple_timing_results.json")
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")
    print("🏆 Simple timing test complete!")

if __name__ == "__main__":
    main()