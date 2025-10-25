#!/usr/bin/env python3
"""
JSON to SQL assembler for baseline execution (no MV optimization).
"""

COL_TYPES = {
    "ts": "int", "type": "str", "auction_id": "str", "advertiser_id": "int", "publisher_id": "int",
    "bid_price": "float", "user_id": "int", "total_price": "float", "country": "str",
    "week": "ts", "day": "date", "hour": "ts", "minute": "str"
}


def _lit(col, val):
    """Convert value to SQL literal with type awareness."""
    t = COL_TYPES.get(col, "str")
    if t in ("int", "float"):
        return str(val)
    if t == "date":
        return f"DATE '{val}'"
    if t == "ts":
        return f"TIMESTAMP '{val}'"
    return "'{}'".format(str(val).replace("'", "''"))


def _where_to_sql(where):
    """Convert JSON where clause to SQL WHERE clause."""
    if not where:
        return ""
    parts = []
    for c in where:
        op = c["op"]
        col = c["col"]
        val = c["val"]
        # Cast day column from VARCHAR (partition column) to DATE for comparisons
        col_expr = f"CAST({col} AS DATE)" if col == "day" else col
        
        if op == "eq":
            parts.append(f"{col_expr} = {_lit(col, val)}")
        elif op == "neq":
            parts.append(f"{col_expr} != {_lit(col, val)}")
        elif op in ("lt", "lte", "gt", "gte"):
            sym = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[op]
            parts.append(f"{col_expr} {sym} {_lit(col, val)}")
        elif op == "between":
            lo, hi = val
            parts.append(f"{col_expr} BETWEEN {_lit(col, lo)} AND {_lit(col, hi)}")
        elif op == "in":
            vs = ", ".join(_lit(col, v) for v in val)
            parts.append(f"{col_expr} IN ({vs})")
    return "WHERE " + " AND ".join(parts)


def _select_to_sql(select):
    """Convert JSON select list to SQL SELECT clause."""
    parts = []
    for it in select:
        if isinstance(it, str):
            parts.append(it)
        else:
            for func, col in it.items():
                expr = f"{func.upper()}({col})"
                parts.append(f'{expr} AS "{expr}"')
    return ", ".join(parts)


def _group_by_to_sql(gb):
    """Convert JSON group_by to SQL GROUP BY clause."""
    return "" if not gb else "GROUP BY " + ", ".join(gb)


def _order_by_to_sql(ob):
    """Convert JSON order_by to SQL ORDER BY clause."""
    if not ob:
        return ""
    return "ORDER BY " + ", ".join(f'{o["col"]} {o.get("dir", "asc").upper()}' for o in ob)


def assemble_sql(q):
    """Assemble a JSON query into a SQL string."""
    return " ".join([
        "SELECT", _select_to_sql(q.get("select", [])),
        "FROM", q["from"],
        _where_to_sql(q.get("where")),
        _group_by_to_sql(q.get("group_by")),
        _order_by_to_sql(q.get("order_by"))
    ]).strip()


if __name__ == "__main__":
    # Test
    test_query = {
        "from": "events",
        "select": ["day", {"SUM": "bid_price"}],
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2025-01-01", "2025-01-31"]}
        ],
        "group_by": ["day"]
    }
    print(assemble_sql(test_query))
