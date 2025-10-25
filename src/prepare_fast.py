#!/usr/bin/env python3
"""
Fast streaming prepare - writes Parquet lake directly from CSV without intermediate table.
"""
import argparse
import duckdb
from pathlib import Path


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

    print("[prepare] Building materialized views from Parquet lake...")
    
    # MV 1: Daily impression revenue
    print("  [1/5] mv_day_impr_revenue")
    con.execute(f"""
        COPY (
          SELECT day, SUM(bid_price) AS sum_bid, COUNT(*) AS events
          FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
          WHERE type='impression'
          GROUP BY day
        )
        TO '{args.mvs}/mv_day_impr_revenue'
        (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 2: Publisher revenue by country/day  
    print("  [2/5] mv_day_country_publisher_impr")
    con.execute(f"""
        COPY (
          SELECT day, country, publisher_id, SUM(bid_price) AS sum_bid, COUNT(*) AS events
          FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
          WHERE type='impression'
          GROUP BY ALL
        )
        TO '{args.mvs}/mv_day_country_publisher_impr'
        (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 3: Average purchase by country
    print("  [3/5] mv_country_purchase_avg")
    con.execute(f"""
        COPY (
          SELECT country,
                 AVG(total_price) AS avg_total,
                 SUM(CASE WHEN total_price IS NOT NULL THEN 1 ELSE 0 END) AS n
          FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
          WHERE type='purchase'
          GROUP BY country
        )
        TO '{args.mvs}/mv_country_purchase_avg'
        (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 4: Advertiser type counts
    print("  [4/5] mv_adv_type_counts")
    con.execute(f"""
        COPY (
          SELECT advertiser_id, type, COUNT(*) AS n
          FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
          GROUP BY advertiser_id, type
        )
        TO '{args.mvs}/mv_adv_type_counts'
        (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 5: Minute-level impression spend
    print("  [5/5] mv_day_minute_impr")
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

    print(f"\n✓ Prepared Parquet lake: {args.lake}")
    print(f"✓ Materialized rollups: {args.mvs}")


if __name__ == "__main__":
    main()
