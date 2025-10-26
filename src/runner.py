#!/usr/bin/env python3
"""
Optimized runner: Parse JSON queries, use planner to choose MV, execute, output CSV + report.
"""
import argparse
import time
import json
import duckdb
import orjson
from pathlib import Path
from shutil import copyfile
from planner import choose_plan
from planner_cache import fingerprint
from router_telemetry import get_telemetry, MVRoutingStatus, FallbackReason, reset_telemetry


def lit(col, val):
    """Convert value to SQL literal with type awareness."""
    # Type inference based on column name - use DATE/TIMESTAMP constructors
    if col in ("day", "week", "hour"):
        if col == "day":
            return f"DATE '{val}'"
        return f"TIMESTAMP '{val}'"
    if isinstance(val, (int, float)):
        return str(val)
    # String literal
    sval = str(val).replace("'", "''")
    return f"'{sval}'"



def build_where(where, keep_cols):
    """Build WHERE clause from JSON filters, keeping only specified columns."""
    parts = []
    for c in where or []:
        # If keep_cols is specified (not None) and not empty, filter by it
        # If keep_cols is empty list, skip all filters
        if keep_cols is not None and c["col"] not in keep_cols:
            continue
        
        op = c["op"]
        col = c["col"]
        # Cast day column from VARCHAR (partition column) to DATE for comparisons
        col_expr = f"CAST({col} AS DATE)" if col == "day" else col
        
        if op in ("eq", "neq"):
            sym = "=" if op == "eq" else "!="
            parts.append(f"{col_expr} {sym} {lit(col, c['val'])}")
        elif op in ("lt", "lte", "gt", "gte"):
            sym = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[op]
            parts.append(f"{col_expr} {sym} {lit(col, c['val'])}")
        elif op == "between":
            lo, hi = c["val"]
            # Handle date literals consistently
            lo_lit = lit(col, lo)
            hi_lit = lit(col, hi)
            parts.append(f"{col_expr} BETWEEN {lo_lit} AND {hi_lit}")
        elif op == "in":
            vals = ", ".join(lit(col, v) for v in c["val"])
            parts.append(f"{col_expr} IN ({vals})")
    
    return ("WHERE " + " AND ".join(parts)) if parts else ""


def project_from_json(q, table, measure=None):
    """Build SELECT clause with direct column projection for wide MVs."""
    out = []
    for it in q.get("select", []):
        if isinstance(it, str):
            out.append(it)
        else:
            func, col = next(iter(it.items()))
            F = func.upper()
            
            # All-time counts MV (direct column mapping)
            if table == "mv_all_adv_type_counts" and F == "COUNT" and col == "*":
                out.append('cnt AS "COUNT(*)"')
            
            # Wide MVs need aggregation since they're partitioned
            elif table.endswith("_wide"):
                if measure == "impr" and F == "SUM" and col == "bid_price":
                    out.append('SUM(sum_bid_impr) AS "SUM(bid_price)"')
                elif measure == "purchase" and F == "AVG" and col == "total_price":
                    out.append('SUM(sum_total_pur) / NULLIF(SUM(cnt_total_pur), 0) AS "AVG(total_price)"')
                elif measure == "count" and F == "COUNT":
                    out.append('SUM(events_all) AS "COUNT(*)"')
                # Handle total_price aggregates on wide MVs
                elif F == "SUM" and col == "total_price":
                    alias_name = it.get("as", f"{F}({col})")
                    out.append(f'SUM(sum_total_pur) AS "{alias_name}"')
                elif F == "AVG" and col == "total_price":
                    alias_name = it.get("as", f"{F}({col})")
                    out.append(f'SUM(sum_total_pur) / NULLIF(SUM(cnt_total_pur), 0) AS "{alias_name}"')
                elif F == "COUNT" and col == "*":
                    # Handle explicit alias if present
                    if "as" in it:
                        alias_name = it["as"]
                        out.append(f'SUM(events_all) AS "{alias_name}"')
                    else:
                        out.append('SUM(events_all) AS "COUNT(*)"')
                # MIN/MAX not pre-computed in wide MVs - force fallback to events_v
                elif F in ["MIN", "MAX"] and col == "total_price":
                    alias_name = it.get("as", f"{F}({col})")
                    out.append(f'{F}({col}) AS "{alias_name}"')  # This will force fallback
                else:
                    # Safe fallback for unexpected combinations
                    alias_name = it.get("as", f"{F}({col})")
                    out.append(f'{F}({col}) AS "{alias_name}"')
            
            # Legacy MV mappings (for backward compatibility)
            elif table == "mv_day_country_publisher_impr" and F == "SUM" and col == "bid_price":
                out.append('SUM(sum_bid) AS "SUM(bid_price)"')
            elif table == "mv_day_minute_impr" and F == "SUM" and col == "bid_price":
                out.append('SUM(sum_bid) AS "SUM(bid_price)"')
            else:
                # Standard aggregation
                out.append(f'{F}({col}) AS "{F}({col})"')
    
    return ", ".join(out) if out else "*"


def build_select(select_items, table=None, measure=None):
    """Legacy wrapper - use project_from_json for new code."""
    # Convert to query format for project_from_json
    q = {"select": select_items}
    return project_from_json(q, table, measure)


def build_order_by(order_by, select_items=None):
    """Build ORDER BY clause, resolving aliases from SELECT if needed."""
    if not order_by:
        return ""
    
    # Build alias mapping from SELECT items
    alias_map = {}
    if select_items:
        for item in select_items:
            if isinstance(item, dict):
                # Handle aggregate functions with optional alias
                if "as" in item:
                    alias_name = item["as"]
                    # Find the function and column (skip the 'as' key)
                    for key, val in item.items():
                        if key != "as":
                            func, col = key, val
                            alias_map[alias_name] = f'{func.upper()}({col})'
                            break
                else:
                    # No explicit alias - use function(column) format
                    for func, col in item.items():
                        alias_map[f'{func.upper()}({col})'] = f'{func.upper()}({col})'
                        break
    
    parts = []
    for o in order_by:
        col = o['col']
        # Resolve alias if it exists in the alias map
        resolved_col = alias_map.get(col, col)
        parts.append(f"{resolved_col} {o.get('dir','asc').upper()}")
    
    return "ORDER BY " + ", ".join(parts)


def run_query(con, q, out_path, cache_dir: Path):
    """Execute a JSON query and write results to CSV (with plan/result cache)."""
    telemetry = get_telemetry()
    query_name = Path(out_path).stem
    
    # Result cache lookup
    fp = fingerprint(q)
    cache_file = cache_dir / f"{fp}.csv"
    if cache_file.exists():
        copyfile(cache_file, out_path)
        return "cache", 0.0

    # Record routing decision with timing
    routing_start = time.perf_counter()
    table, proj = choose_plan(q)
    routing_time_ms = (time.perf_counter() - routing_start) * 1000
    
    # Determine routing status and fallback reason
    status = MVRoutingStatus.HIT if table != "events_v" else MVRoutingStatus.FALLBACK
    fallback_reason = FallbackReason.NO_MV_MATCH if status == MVRoutingStatus.FALLBACK else None
    
    # Record telemetry (mv_candidates would come from planner in full implementation)
    tracking_id = telemetry.record_routing_decision(
        query_file=query_name,
        selected_table=table,
        mv_candidates=["mv_day_type_wide", "mv_day_country_wide"],  # Simplified for now
        routing_time_ms=routing_time_ms,
        status=status,
        fallback_reason=fallback_reason
    )
    
    keep = proj.get("keep_where", [])
    
    # Build SQL
    measure = proj.get("measure")
    select_sql = build_select(q.get("select", []), table, measure)
    where_sql = build_where(q.get("where"), keep)
    group_by = q.get("group_by", [])

    skip_group_by_for = ["mv_country_purchase_avg"]
    if table in skip_group_by_for:
        group_sql = ""
    else:
        group_sql = f"GROUP BY {', '.join(group_by)}" if group_by else ""

    order_sql = build_order_by(q.get("order_by"), q.get("select"))
    limit_sql = f"LIMIT {int(q.get('limit'))}" if q.get("limit") else ""
    
    sql = f"SELECT {select_sql} FROM {table} {where_sql} {group_sql} {order_sql} {limit_sql}".strip()
    
    # Execute and time
    t0 = time.perf_counter()
    con.execute(f"COPY ({sql}) TO '{out_path}' WITH (HEADER, DELIMITER ',');")
    dt = time.perf_counter() - t0
    execution_time_ms = dt * 1000
    
    # Update telemetry with execution metrics
    telemetry.update_execution_metrics(
        tracking_id=tracking_id,
        execution_time_ms=execution_time_ms
    )
    
    # Export sidecar telemetry file
    telemetry.export_sidecar(out_path, query_name)

    # Store in result cache
    try:
        copyfile(out_path, cache_file)
    except Exception:
        pass
    
    return table, dt


def main():
    ap = argparse.ArgumentParser(description="Execute JSON queries with MV optimization")
    ap.add_argument("--lake", required=True, help="Path to Parquet lake")
    ap.add_argument("--mvs", required=True, help="Path to materialized views")
    ap.add_argument("--queries", required=True, help="Directory of JSON query files")
    ap.add_argument("--out", required=True, help="Output directory for results")
    ap.add_argument("--threads", type=int, default=8, help="DuckDB thread count")
    ap.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    ap.add_argument("--telemetry", action="store_true", help="Enable router telemetry tracking")
    ap.add_argument("--approx-sample", type=float, default=0.0, help="If >0, apply BERNOULLI sampling percent to events_v fallbacks (ad-hoc)")
    ap.add_argument("--approx-seed", type=int, default=42, help="Repeatable sampling seed")
    args = ap.parse_args()

    # Clamp sample percent
    if args.approx_sample < 0:
        args.approx_sample = 0.0
    if args.approx_sample > 100:
        args.approx_sample = 100.0
    
    # Initialize telemetry if requested
    if args.telemetry:
        reset_telemetry()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = out_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    print(f"[runner] Connecting to DuckDB (threads={args.threads}, mem={args.mem})")
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={args.threads};")
        con.execute(f"SET memory_limit='{args.mem}';")
        con.execute(f"PRAGMA temp_directory='{(out_dir / '.ducktmp').absolute()}';")
        con.execute("PRAGMA enable_object_cache=true;")  # Speed repeated file/metadata access
        con.execute("PRAGMA enable_profiling=false;")    # Reduce overhead
    except duckdb.Error:
        pass

    # Base events view will be set per-batch using manifest pruning (fallback to global)
    print(f"[runner] Preparing events view setup for {args.lake}")
    days_map = {}
    try:
        from manifest import load_manifest, patterns_for_where
        days_map = load_manifest(args.lake)
    except Exception:
        days_map = {}

    def days_from_where(where):
        """Extract specific days from WHERE clause for partition pruning."""
        days = []
        for c in where or []:
            if c["col"] == "day":
                if c["op"] == "eq":
                    days = [str(c["val"])]
                elif c["op"] == "between":
                    try:
                        import pandas as pd
                        lo, hi = c["val"]
                        days = [str(d.date()) for d in pd.date_range(lo, hi, freq="D")]
                    except Exception:
                        days = []  # Fallback to full scan
        return days
    
    def set_events_view_for(where_clause: list):
        sample_clause = f" TABLESAMPLE BERNOULLI({args.approx_sample}) REPEATABLE({args.approx_seed})" if args.approx_sample and args.approx_sample > 0 else ""
        # Try partition pruning first (faster than manifest)
        day_list = days_from_where(where_clause)
        if day_list:
            paths = [f"'{args.lake}/events/day={d}/**/*.parquet'" for d in day_list]
            paths_str = "[" + ", ".join(paths) + "]"
            con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet({paths_str}){sample_clause};")
            return
            
        # Use manifest to prune days; fallback to global pattern  
        if days_map:
            pats = patterns_for_where(days_map, where_clause)
        else:
            pats = None
        if pats:
            # Prefer a single read_parquet over UNION for compatibility with sampling
            paths = [f"'{p}'" for p in pats]
            paths_str = "[" + ", ".join(paths) + "]"
            con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet({paths_str}){sample_clause};")
        else:
            con.execute(f"CREATE OR REPLACE VIEW events_v AS SELECT * FROM read_parquet('{args.lake}/events/day=*/**/*.parquet'){sample_clause};")

    # Build MV catalog (no loading into memory)
    print(f"[runner] Cataloging materialized views from {args.mvs}")
    
    mv_candidates = [
        "mv_day_minute_impr",
        "mv_day_country_publisher_impr",
        "mv_all_adv_type_counts",  # 2D all-time counts
        # Wide family
        "mv_day_advertiser_id_wide", "mv_hour_advertiser_id_wide", "mv_week_advertiser_id_wide",
        "mv_day_publisher_id_wide",  "mv_hour_publisher_id_wide",  "mv_week_publisher_id_wide",
        "mv_day_country_wide",       "mv_hour_country_wide",       "mv_week_country_wide", 
        "mv_day_type_wide",          "mv_hour_type_wide",          "mv_week_type_wide",
        "mv_day_user_id_wide",       "mv_hour_user_id_wide",       "mv_week_user_id_wide",
    ]
    
    # Catalog: mv_name -> parquet glob
    mv_glob = {}
    for name in mv_candidates:
        p = Path(args.mvs) / name
        if p.exists():
            # Handle both partitioned (directory) and non-partitioned (file) MVs
            if p.is_dir():
                pattern = f"{p}/**/*.parquet"
            else:
                pattern = str(p)
            mv_glob[name] = pattern
            print(f"  ✓ {name}")
    
    def from_clause_for(table_name: str) -> str:
        """Get FROM clause for table - MV or events_v."""
        if table_name in mv_glob:
            return f"read_parquet('{mv_glob[table_name]}')"
        return "events_v"
    # Run queries with simple batching: group by (table, kept-where, group_by, order_by, limit)
    print(f"\n[runner] Executing queries from {args.queries}")
    metrics = []
    query_files = sorted(Path(args.queries).glob("*.json"))

    # Load and plan all with telemetry
    planned = []
    telemetry = get_telemetry() if args.telemetry else None
    for p in query_files:
        q = orjson.loads(p.read_bytes())
        
        # Track routing decision if telemetry enabled
        routing_start = time.perf_counter()
        table, proj = choose_plan(q)
        routing_time_ms = (time.perf_counter() - routing_start) * 1000
        
        if telemetry:
            status = MVRoutingStatus.HIT if table != "events_v" else MVRoutingStatus.FALLBACK
            fallback_reason = FallbackReason.NO_MV_MATCH if status == MVRoutingStatus.FALLBACK else None
            tracking_id = telemetry.record_routing_decision(
                query_file=p.stem,
                selected_table=table,
                mv_candidates=list(mv_glob.keys()),
                routing_time_ms=routing_time_ms,
                status=status,
                fallback_reason=fallback_reason
            )
        
        planned.append({
            "path": p, 
            "q": q, 
            "table": table, 
            "keep": proj.get("keep_where", []),
            "measure": proj.get("measure"),
            "tracking_id": tracking_id if telemetry else None
        })

    # Group key helpers
    import json as _json
    def where_key(q, keep):
        wh = [w for w in (q.get("where") or []) if (not keep) or w.get("col") in keep]
        wh = sorted(wh, key=lambda w: (w.get("col"), w.get("op"), _json.dumps(w.get("val"), sort_keys=True)))
        return _json.dumps(wh, sort_keys=True)

    def order_key(q):
        ob = q.get("order_by") or []
        return tuple((o.get("col"), (o.get("dir") or "asc").lower()) for o in ob)

    from collections import defaultdict
    groups = defaultdict(list)
    for item in planned:
        q = item["q"]
        key = (
            item["table"],
            where_key(q, item["keep"]),
            tuple(sorted(q.get("group_by") or [])),
            order_key(q),
            int(q.get("limit")) if q.get("limit") else None,
        )
        groups[key].append(item)

    # Execute each group
    for gi, (gkey, items) in enumerate(groups.items(), 1):
        table, _, gb_sorted, ob_key, lim = gkey
        # Set events view for this group's filters if scanning lake
        first_q = items[0]["q"]
        if table == "events_v":
            set_events_view_for(first_q.get("where") or [])

        # Union select list across group
        select_union = []
        seen = set()
        for it in items:
            for s in it["q"].get("select", []):
                key = _json.dumps(s, sort_keys=True)
                if key not in seen:
                    select_union.append(s)
                    seen.add(key)
        # Use original (unsorted) group_by if present in first query
        group_by = first_q.get("group_by") or list(gb_sorted)

        # Build SQL for the superset using new projection logic
        measure = items[0].get("measure")
        select_sql = project_from_json({"select": select_union}, table, measure)
        where_sql = build_where(first_q.get("where"), items[0]["keep"])  # same kept filters in group
        
        # Skip GROUP BY for pre-aggregated all-time MVs (data is already grouped)
        if table == "mv_all_adv_type_counts":
            group_sql = ""  # Already pre-aggregated by advertiser_id, type
        else:
            group_by = first_q.get("group_by") or list(gb_sorted)
            group_sql = f"GROUP BY {', '.join(group_by)}" if group_by else ""
            
        order_sql = build_order_by([{ "col": c, "dir": d } for c, d in ob_key], first_q.get("select")) if ob_key else ""
        limit_sql = f"LIMIT {lim}" if lim else ""
        
        # Use lazy FROM clause (no memory loading)
        src_table = from_clause_for(table)
        sup_sql = f"SELECT {select_sql} FROM {src_table} {where_sql} {group_sql} {order_sql} {limit_sql}".strip()
        tmp_csv = out_dir / f".batch_group_{gi}.csv"

        # Execute superset query with profiling if available
        t0 = time.perf_counter()
        if telemetry:
            con.execute("PRAGMA enable_profiling;")
        con.execute(f"COPY ({sup_sql}) TO '{tmp_csv}' WITH (HEADER, DELIMITER ',');")
        dt_sup = time.perf_counter() - t0
        execution_time_ms = dt_sup * 1000
        
        # Extract profiling info if enabled
        profile_info = None
        if telemetry:
            try:
                profile_result = con.execute("PRAGMA profiling_output;").fetchall()
                if profile_result:
                    profile_info = str(profile_result[0][0])
                con.execute("PRAGMA disable_profiling;")
            except Exception:
                pass

        # Now carve per-query outputs (subset columns)
        # Register the temp CSV as a view for projection
        con.execute(f"CREATE OR REPLACE VIEW __batch_v AS SELECT * FROM read_csv_auto('{tmp_csv}', header=true);")
        # Build column name sets per-query based on desired select aliasing
        for idx, it in enumerate(items, 1):
            q = it["q"]
            out_file = out_dir / f"{it['path'].stem}.csv"
            # Build projection based on columns that exist in the batched result
            proj_cols = []
            for item in q.get("select", []):
                if isinstance(item, str):
                    proj_cols.append(item)
                else:
                    # For aggregates, check if there's an explicit alias
                    if "as" in item:
                        # Use the explicit alias name
                        alias = item["as"]
                        proj_cols.append(f'"{alias}"')
                    else:
                        # Use function(column) format
                        for func, col in item.items():
                            if func != "as":
                                alias = f'"{func.upper()}({col})"'
                                proj_cols.append(alias)
                                break
            
            proj_sql = f"SELECT {', '.join(proj_cols)} FROM __batch_v"
            con.execute(f"COPY ({proj_sql}) TO '{out_file}' WITH (HEADER, DELIMITER ',');")
            
            # Update telemetry for this query
            if telemetry and it.get("tracking_id"):
                telemetry.update_execution_metrics(
                    tracking_id=it["tracking_id"],
                    execution_time_ms=execution_time_ms if idx == 1 else 0.0
                )
                # Export sidecar telemetry file
                telemetry.export_sidecar(str(out_file), it["path"].stem)
            
            metrics.append({
                "query": it["path"].name,
                "table": table,
                "seconds": round(dt_sup, 4) if idx == 1 else 0.0,
                "output": str(out_file),
                "cache_hit": False,
                "batched": True if len(items) > 1 else False
            })

        # Cleanup temp
        try:
            tmp_csv.unlink(missing_ok=True)
        except Exception:
            pass

    # Write report
    report_path = out_dir / "report.json"
    report_path.write_text(json.dumps({"queries": metrics}, indent=2))
    print(f"\n✓ Report written to {report_path}")
    
    # Export comprehensive telemetry report if enabled
    if telemetry:
        telemetry_path = telemetry.export_batch_report(str(out_dir))
        print(f"✓ Router telemetry written to {telemetry_path}")


if __name__ == "__main__":
    main()
