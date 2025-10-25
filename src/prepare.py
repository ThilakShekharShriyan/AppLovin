#!/usr/bin/env python3
"""
Prepare phase: Build Parquet lake (partitioned by day) and materialized view rollups.
"""
import argparse
import duckdb
from pathlib import Path


def ensure_dirs(*paths):
    """Create directories if they don't exist."""
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)


# Adapt to both formats: ad-events challenge OR existing AppLovin data
DDL = """
CREATE OR REPLACE TABLE events AS
WITH raw AS (
  SELECT * FROM read_csv_auto('{raw}/events_part_*.csv', union_by_name=true, normalize_names=true, ignore_errors=true)
), converted AS (
  SELECT
    -- Handle both formats: ts (epoch millis) OR event_time (timestamp string)
    CASE 
      WHEN TRY_CAST(ts AS BIGINT) IS NOT NULL THEN to_timestamp(CAST(ts AS DOUBLE)/1000.0)
      ELSE NULL
    END AS ts_converted,
    -- Map columns to unified schema
    _type AS type,
    auction_id,
    advertiser_id,
    publisher_id,
    bid_price,
    user_id,
    total_price,
    country,
    advertiser_id AS app_id
  FROM raw
)
SELECT
  ts_converted AS ts,
  DATE_TRUNC('week', ts_converted) AS week,
  DATE_TRUNC('day', ts_converted) AS day,
  DATE_TRUNC('hour', ts_converted) AS hour,
  STRFTIME(ts_converted, '%Y-%m-%d %H:%M') AS minute,
  type,
  auction_id,
  TRY_CAST(advertiser_id AS INTEGER) AS advertiser_id,
  TRY_CAST(publisher_id AS INTEGER) AS publisher_id,
  bid_price,
  TRY_CAST(user_id AS BIGINT) AS user_id,
  total_price,
  country
FROM converted
WHERE ts_converted IS NOT NULL;
"""

PARQUET_OUT = """
COPY (SELECT * FROM events)
TO '{lake}/events'
(FORMAT PARQUET, PARTITION_BY (day), OVERWRITE_OR_IGNORE TRUE,
 COMPRESSION ZSTD, ROW_GROUP_SIZE 268435456);
"""

# Optional: apps_dim if it exists
APPS_DIM = """
CREATE OR REPLACE TABLE apps_dim AS
SELECT * FROM read_csv_auto('{raw}/apps_dim.csv', union_by_name=true, normalize_names=true, ignore_errors=true);

COPY (SELECT * FROM apps_dim)
TO '{lake}/apps_dim.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
"""


def build_rollups(con, lake, mvs):
    """Build all materialized view rollups."""
    print("[prepare] Building materialized views...")
    
    # MV 1: Daily revenue from impressions
    print("  [1/5] mv_day_impr_revenue")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_day_impr_revenue AS
    SELECT day, SUM(bid_price) AS sum_bid, COUNT(*) AS events
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    WHERE type='impression'
    GROUP BY day;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_day_impr_revenue)
    TO '{mvs}/mv_day_impr_revenue'
    (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 2: Publisher revenue in country/day (impressions)
    print("  [2/5] mv_day_country_publisher_impr")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_day_country_publisher_impr AS
    SELECT day, country, publisher_id, SUM(bid_price) AS sum_bid, COUNT(*) AS events
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    WHERE type='impression'
    GROUP BY ALL;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_day_country_publisher_impr)
    TO '{mvs}/mv_day_country_publisher_impr'
    (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 3: Average purchase by country (all time)
    print("  [3/5] mv_country_purchase_avg")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_country_purchase_avg AS
    SELECT country,
           AVG(total_price) AS avg_total,
           SUM(CASE WHEN total_price IS NOT NULL THEN 1 ELSE 0 END) AS n
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    WHERE type='purchase'
    GROUP BY country;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_country_purchase_avg)
    TO '{mvs}/mv_country_purchase_avg'
    (FORMAT PARQUET, PARTITION_BY (country), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 4: Events per advertiser per type (all time)
    print("  [4/5] mv_adv_type_counts")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_adv_type_counts AS
    SELECT advertiser_id, type, COUNT(*) AS n
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    GROUP BY advertiser_id, type;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_adv_type_counts)
    TO '{mvs}/mv_adv_type_counts'
    (FORMAT PARQUET, PARTITION_BY (type), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 5: Minute grain spend on a given day (impressions)
    print("  [5/5] mv_day_minute_impr")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_day_minute_impr AS
    SELECT day, minute, SUM(bid_price) AS sum_bid, COUNT(*) AS events
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    WHERE type='impression'
    GROUP BY day, minute;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_day_minute_impr)
    TO '{mvs}/mv_day_minute_impr'
    (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)


def main():
    ap = argparse.ArgumentParser(description="Prepare Parquet lake and materialized views")
    ap.add_argument("--raw", required=True, help="Path to raw CSV files")
    ap.add_argument("--lake", required=True, help="Output path for Parquet lake")
    ap.add_argument("--mvs", required=True, help="Output path for materialized views")
    ap.add_argument("--threads", type=int, default=8, help="DuckDB thread count")
    ap.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    args = ap.parse_args()

    ensure_dirs(args.lake, args.mvs)

    print(f"[prepare] Connecting to DuckDB (threads={args.threads}, mem={args.mem})")
    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={args.threads};")
    con.execute(f"SET memory_limit='{args.mem}';")
    # Increase temp directory size for large datasets
    con.execute("PRAGMA max_temp_directory_size='50GiB';")
    con.execute(f"PRAGMA temp_directory='{Path(args.lake).absolute()}/.ducktmp';")

    print(f"[prepare] Loading events from {args.raw}/events_part_*.csv")
    con.execute(DDL.format(raw=args.raw))
    
    print(f"[prepare] Writing Parquet lake to {args.lake}")
    con.execute(PARQUET_OUT.format(lake=args.lake))
    
    # Optional: apps_dim
    apps_dim_path = Path(args.raw) / "apps_dim.csv"
    if apps_dim_path.exists():
        print(f"[prepare] Writing apps_dim to {args.lake}/apps_dim.parquet")
        con.execute(APPS_DIM.format(raw=args.raw, lake=args.lake))

    build_rollups(con, args.lake, args.mvs)

    # Analyze tables (best-effort)
    print("[prepare] Analyzing tables...")
    for t in ["events", "mv_day_impr_revenue", "mv_day_country_publisher_impr",
              "mv_country_purchase_avg", "mv_adv_type_counts", "mv_day_minute_impr"]:
        try:
            con.execute(f"ANALYZE {t};")
        except duckdb.Error:
            pass

    print(f"\n✓ Prepared Parquet lake: {args.lake}")
    print(f"✓ Materialized rollups: {args.mvs}")


if __name__ == "__main__":
    main()
