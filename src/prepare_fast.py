#!/usr/bin/env python3
"""
Fast streaming prepare - writes Parquet lake directly from CSV without intermediate table.
"""
import argparse
import duckdb
from pathlib import Path


def build_mv_family_wide(con, lake, mvs, max_rows=80_000_000, include_user=True):
    """Build general MV family (time grain × 1 dimension) using filtered aggregates."""
    # Estimate cardinalities for size planning
    stats = {}
    dims = ["advertiser_id", "publisher_id", "country", "type"] + (["user_id"] if include_user else [])
    
    print("[mv] Estimating cardinalities...")
    for d in dims:
        try:
            n = con.execute(f"SELECT COUNT(DISTINCT {d}) FROM read_parquet('{lake}/events/day=*/**/*.parquet')").fetchone()[0]
            stats[d] = int(n)
        except Exception:
            stats[d] = 1000000  # Conservative fallback
    
    # Get time range for bucket estimation
    try:
        mn, mx = con.execute(f"SELECT MIN(day), MAX(day) FROM read_parquet('{lake}/events/day=*/**/*.parquet')").fetchone()
        n_days = (mx - mn).days + 1 if mn and mx else 365
        n_hours = n_days * 24
        n_weeks = (mx - mn).days // 7 + 1 if mn and mx else 52
    except Exception:
        n_days, n_hours, n_weeks = 365, 8760, 52
    
    grains = [
        ("day", n_days, "day"),
        ("hour", n_hours, "hour"), 
        ("week", n_weeks, "week")
    ]
    
    def ok(dim, buckets): 
        return stats[dim] * buckets <= max_rows
    
    built = []
    for grain, buckets, time_col in grains:
        for dim in dims:
            if not ok(dim, buckets):
                print(f"[skip] mv_{grain}_{dim}_wide (est {stats[dim]*buckets:,} rows > cap)")
                continue
                
            mv = f"mv_{grain}_{dim}_wide"
            print(f"[building] {mv} (est {stats[dim]*buckets:,} rows)")
            
            # Include country as an additional dimension to support country filtering
            select_cols = (
                f"{time_col} AS {grain}, {dim}, country, " if dim != "country" else f"{time_col} AS {grain}, country, "
            )
            group_cols = (f"{grain}, {dim}, country" if dim != "country" else f"{grain}, country")
            
            sql = f"""
                COPY (
                  SELECT {select_cols}
                         -- Filtered aggregates (one wide row supports many query types)
                         SUM(bid_price) FILTER (WHERE type='impression') AS sum_bid_impr,
                         SUM(total_price) FILTER (WHERE type='purchase') AS sum_total_pur,
                         COUNT(*) FILTER (WHERE type='purchase' AND total_price IS NOT NULL) AS cnt_total_pur,
                         COUNT(*) AS events_all
                  FROM read_parquet('{lake}/events/day=*/**/*.parquet')
                  GROUP BY {group_cols}
                )
                TO '{mvs}/{mv}'
                (FORMAT PARQUET, PARTITION_BY ({grain}), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
            """
            con.execute(sql)
            built.append(mv)
    
    print(f"[mv] Built wide MVs: {', '.join(built)}")
    return built


def main():
    ap = argparse.ArgumentParser(description="Fast streaming Parquet lake preparation")
    ap.add_argument("--raw", required=True, help="Path to raw CSV files")
    ap.add_argument("--lake", required=True, help="Output path for Parquet lake")
    ap.add_argument("--mvs", required=True, help="Output path for materialized views")
    ap.add_argument("--threads", type=int, default=8, help="DuckDB thread count")
    ap.add_argument("--mem", default="8GB", help="DuckDB memory limit")
    args = ap.parse_args()

    Path(args.lake).mkdir(parents=True, exist_ok=True)
    Path(args.mvs).mkdir(parents=True, exist_ok=True)

    print(f"[prepare] Connecting to DuckDB (threads={args.threads}, mem={args.mem})")
    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={args.threads};")
    con.execute(f"SET memory_limit='{args.mem}';")
    con.execute("PRAGMA max_temp_directory_size='50GiB';")
    con.execute(f"PRAGMA temp_directory='{Path(args.lake).absolute()}/.ducktmp';")

    # Stream CSV → Parquet directly (no intermediate table)
    print(f"[prepare] Streaming {args.raw}/events_part_*.csv → {args.lake}/events")
    con.execute(f"""
        COPY (
          SELECT
            to_timestamp(CAST(ts AS DOUBLE)/1000.0) AS ts,
            DATE_TRUNC('week', to_timestamp(CAST(ts AS DOUBLE)/1000.0)) AS week,
            DATE_TRUNC('day', to_timestamp(CAST(ts AS DOUBLE)/1000.0)) AS day,
            DATE_TRUNC('hour', to_timestamp(CAST(ts AS DOUBLE)/1000.0)) AS hour,
            STRFTIME(to_timestamp(CAST(ts AS DOUBLE)/1000.0), '%Y-%m-%d %H:%M') AS minute,
            "type",
            auction_id,
            TRY_CAST(advertiser_id AS INTEGER) AS advertiser_id,
            TRY_CAST(publisher_id AS INTEGER) AS publisher_id,
            TRY_CAST(bid_price AS DOUBLE) AS bid_price,
            TRY_CAST(user_id AS BIGINT) AS user_id,
            TRY_CAST(total_price AS DOUBLE) AS total_price,
            country
          FROM read_csv_auto('{args.raw}/events_part_*.csv', union_by_name=true, normalize_names=false)
        )
        TO '{args.lake}/events'
        (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE, ROW_GROUP_SIZE 268435456);
    """)

    print("[prepare] Building wide MV family with filtered aggregates...")
    build_mv_family_wide(con, args.lake, args.mvs, max_rows=80_000_000, include_user=True)
    
    # Keep specific 2-D MV for publisher+country queries
    print("[prepare] Building specific 2D MVs...")
    con.execute(f"""
        COPY (
          SELECT day, country, publisher_id,
                 SUM(bid_price) FILTER (WHERE type='impression') AS sum_bid_impr,
                 COUNT(*) FILTER (WHERE type='impression') AS cnt_impr
          FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
          GROUP BY ALL
        )
        TO '{args.mvs}/mv_day_country_publisher_impr'
        (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)
    
    # Keep minute-level for high-precision queries
    con.execute(f"""
        COPY (
          SELECT day, minute, SUM(bid_price) AS sum_bid, COUNT(*) AS events
          FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
          WHERE type='impression'
          GROUP BY day, minute
        )
        TO '{args.mvs}/mv_day_minute_impr'
        (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)
    
    # Add tiny 2D MV for advertiser×type counts (eliminates q4 bottleneck)
    print("[prepare] Building all-time advertiser×type counts MV...")
    con.execute(f"""
        CREATE OR REPLACE TABLE mv_all_adv_type_counts AS
        SELECT advertiser_id, type, COUNT(*) AS cnt
        FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
        GROUP BY advertiser_id, type;
    """)
    con.execute(f"""
        COPY (SELECT * FROM mv_all_adv_type_counts)
        TO '{args.mvs}/mv_all_adv_type_counts'
        (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    print(f"\n✓ Prepared Parquet lake: {args.lake}")
    print(f"✓ Materialized rollups: {args.mvs}")


if __name__ == "__main__":
    main()
