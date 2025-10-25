#!/usr/bin/env python3
"""
Build only the 3 wider MVs without rebuilding lake or original MVs.
Use this when you already have the lake and want to add wider MVs.
"""
import argparse
import duckdb
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description="Add wider MVs to existing lake")
    ap.add_argument("--lake", required=True, help="Path to existing Parquet lake")
    ap.add_argument("--mvs", required=True, help="Output path for materialized views")
    ap.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    ap.add_argument("--mem", default="8GB", help="DuckDB memory limit")
    args = ap.parse_args()

    Path(args.mvs).mkdir(parents=True, exist_ok=True)

    print(f"[wider-mvs] Connecting to DuckDB (threads={args.threads}, mem={args.mem})")
    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={args.threads};")
    con.execute(f"SET memory_limit='{args.mem}';")
    
    # Reduce temp space usage
    con.execute("PRAGMA preserve_insertion_order=false;")
    
    print(f"[wider-mvs] Loading events from {args.lake}")
    
    # MV 6: Wide MV - Hourly revenue by country + publisher + advertiser (impressions)
    print("  [1/3] mv_hour_country_pub_adv_impr (wide MV)")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_hour_country_pub_adv_impr AS
    SELECT 
        hour,
        day,
        country,
        publisher_id,
        advertiser_id,
        SUM(bid_price) AS sum_bid,
        COUNT(*) AS events
    FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
    WHERE type='impression'
    GROUP BY ALL;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_hour_country_pub_adv_impr)
    TO '{args.mvs}/mv_hour_country_pub_adv_impr'
    (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)
    print(f"    ✓ Saved to {args.mvs}/mv_hour_country_pub_adv_impr")
    
    # MV 7: Wide MV - Daily events by country + type + advertiser (all events)
    print("  [2/3] mv_day_country_type_adv (wide MV)")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_day_country_type_adv AS
    SELECT 
        day,
        country,
        type,
        advertiser_id,
        publisher_id,
        COUNT(*) AS events,
        SUM(bid_price) AS sum_bid,
        SUM(total_price) AS sum_total
    FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
    GROUP BY ALL;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_day_country_type_adv)
    TO '{args.mvs}/mv_day_country_type_adv'
    (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)
    print(f"    ✓ Saved to {args.mvs}/mv_day_country_type_adv")
    
    # MV 8: Wide MV - Weekly aggregates by advertiser + country (all event types)
    print("  [3/3] mv_week_adv_country_type (wide MV)")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_week_adv_country_type AS
    SELECT 
        week,
        advertiser_id,
        country,
        type,
        COUNT(*) AS events,
        SUM(bid_price) AS sum_bid,
        SUM(total_price) AS sum_total,
        COUNT(DISTINCT user_id) AS unique_users
    FROM read_parquet('{args.lake}/events/day=*/**/*.parquet')
    GROUP BY ALL;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_week_adv_country_type)
    TO '{args.mvs}/mv_week_adv_country_type'
    (FORMAT PARQUET, PARTITION_BY (week), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)
    print(f"    ✓ Saved to {args.mvs}/mv_week_adv_country_type")

    print(f"\n✓ Successfully added 3 wider MVs to {args.mvs}")
    print(f"✓ Your existing 5 narrow MVs are still in {args.mvs}")
    print(f"\nYou now have 8 total MVs (5 original + 3 wide)")


if __name__ == "__main__":
    main()
