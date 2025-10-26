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


def build_mv_family(con, lake: str, mvs: str, max_rows: int = 50_000_000, include_user: bool = True):
    """Build WIDE MV family: (grain x 1 dimension x country) with unified columns.
    Emits tables named mv_{grain}_{dim}_wide with columns:
      - sum_bid_impr, sum_total_pur, cnt_total_pur, events_all
    Guard with approximate cap: distinct(dim) * buckets(grain) <= max_rows.
    """
    print("[prepare] Building WIDE MV family (grain x 1D x country)...")

    # Estimate distincts per dimension
    dims = ["advertiser_id", "publisher_id", "country", "type"] + (["user_id"] if include_user else [])
    stats = {}
    for dim in dims:
        df = con.execute(
            f"SELECT COUNT(DISTINCT {dim}) AS n FROM read_parquet('{lake}/events/day=*/**/*.parquet')"
        ).df()
        stats[dim] = int(df.iloc[0]["n"]) if not df.empty else 0

    # Estimate time buckets
    rng = con.execute(
        f"SELECT MIN(day) AS min_day, MAX(day) AS max_day FROM read_parquet('{lake}/events/day=*/**/*.parquet')"
    ).df()
    if rng.empty or rng.iloc[0]["min_day"] is None or rng.iloc[0]["max_day"] is None:
        print("[prepare] No data range found for MV family; skipping.")
        return
    min_day = rng.iloc[0]["min_day"]
    max_day = rng.iloc[0]["max_day"]
    n_days = (max_day - min_day).days + 1
    n_weeks = max(1, n_days // 7)
    n_hours = n_days * 24

    grains = [
        ("day", n_days, "day"),
        ("week", n_weeks, "week"),
        ("hour", n_hours, "hour"),
    ]

    def allowed(dim: str, buckets: int) -> bool:
        return stats.get(dim, 0) * buckets <= max_rows

    built = []

    def emit_wide(grain: str, dim: str):
        mv = f"mv_{grain}_{dim}_wide"
        con.execute(f"""
        CREATE OR REPLACE TABLE {mv} AS
        SELECT {grain}, {dim}, country,
               SUM(CASE WHEN type='impression' THEN COALESCE(bid_price, 0) ELSE 0 END) AS sum_bid_impr,
               SUM(CASE WHEN type='purchase' THEN COALESCE(total_price, 0) ELSE 0 END) AS sum_total_pur,
               COUNT(*) FILTER (WHERE type='purchase' AND total_price IS NOT NULL) AS cnt_total_pur,
               COUNT(*) AS events_all
        FROM read_parquet('{lake}/events/day=*/**/*.parquet')
        GROUP BY {grain}, {dim}, country;
        """)
        con.execute(f"""
        COPY (SELECT * FROM {mv})
        TO '{mvs}/{mv}'
        (FORMAT PARQUET, PARTITION_BY ({grain}), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
        """)
        built.append(mv)

    for grain, buckets, _ in grains:
        for dim in dims:
            if allowed(dim, buckets):
                emit_wide(grain, dim)
            else:
                print(f"[prepare] Skip mv_{grain}_{dim}_wide: est rows {stats.get(dim,0)*buckets:,} > cap {max_rows:,}")

    print("[prepare] WIDE MV family built:")
    for name in built[:10]:
        print(f"  ✓ {name}")
    if len(built) > 10:
        print(f"  ... and {len(built)-10} more")

# Optional: apps_dim if it exists
APPS_DIM = """
CREATE OR REPLACE TABLE apps_dim AS
SELECT * FROM read_csv_auto('{raw}/apps_dim.csv', union_by_name=true, normalize_names=true, ignore_errors=true);

COPY (SELECT * FROM apps_dim)
TO '{lake}/apps_dim.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
"""


def build_rollups(con, lake, mvs):
    """Build all materialized view rollups (original + wider MVs)."""
    print("[prepare] Building materialized views...")
    
    # ========== ORIGINAL NARROW MVs (5) ==========
    
    # MV 1: Daily revenue from impressions
    print("  [1/8] mv_day_impr_revenue")
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
    print("  [2/8] mv_day_country_publisher_impr")
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
    print("  [3/8] mv_country_purchase_avg")
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
    print("  [4/8] mv_all_adv_type_counts")
    con.execute(f"""
    CREATE OR REPLACE TABLE mv_all_adv_type_counts AS
    SELECT advertiser_id, type, COUNT(*) AS cnt
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    GROUP BY advertiser_id, type;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_all_adv_type_counts)
    TO '{mvs}/mv_all_adv_type_counts'
    (FORMAT PARQUET, PARTITION_BY (type), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)

    # MV 5: Minute grain spend on a given day (impressions)
    print("  [5/8] mv_day_minute_impr")
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

    # ========== WIDER MVs FOR FLEXIBLE AGGREGATION (3) ==========
    
    # MV 6: Wide MV - Hourly revenue by country + publisher + advertiser (impressions)
    # Supports queries asking for any subset: hourly/daily, by country, by publisher, by advertiser
    print("  [6/8] mv_hour_country_pub_adv_impr (wide MV)")
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
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    WHERE type='impression'
    GROUP BY ALL;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_hour_country_pub_adv_impr)
    TO '{mvs}/mv_hour_country_pub_adv_impr'
    (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)
    
    # MV 7: Wide MV - Daily events by country + type + advertiser (all events)
    # Supports queries asking for event counts across different dimensions
    print("  [7/8] mv_day_country_type_adv (wide MV)")
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
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    GROUP BY ALL;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_day_country_type_adv)
    TO '{mvs}/mv_day_country_type_adv'
    (FORMAT PARQUET, PARTITION_BY (day), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """)
    
    # MV 8: Wide MV - Weekly aggregates by advertiser + country (all event types)
    # Supports weekly/monthly rollups and advertiser analysis
    print("  [8/8] mv_week_adv_country_type (wide MV)")
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
    FROM read_parquet('{lake}/events/day=*/**/*.parquet')
    GROUP BY ALL;
    """)
    con.execute(f"""
    COPY (SELECT * FROM mv_week_adv_country_type)
    TO '{mvs}/mv_week_adv_country_type'
    (FORMAT PARQUET, PARTITION_BY (week), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
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
    # Use /tmp for temporary files (usually has more space)
    import tempfile
    temp_dir = tempfile.gettempdir()
    print(f"[prepare] Using temp directory: {temp_dir}")
    con.execute("PRAGMA max_temp_directory_size='50GiB';")
    con.execute(f"PRAGMA temp_directory='{temp_dir}/duckdb_tmp';")

    print(f"[prepare] Loading events from {args.raw}/events_part_*.csv")
    con.execute(DDL.format(raw=args.raw))
    
    print(f"[prepare] Writing Parquet lake to {args.lake}")
    con.execute(PARQUET_OUT.format(lake=args.lake))

    # Write day manifest for pruning
    try:
        from manifest import write_manifest
        mf = write_manifest(args.lake)
        if mf:
            print(f"[prepare] Wrote manifest: {mf}")
    except Exception as e:
        print(f"[prepare] Warning: manifest write failed: {e}")
    
    # Optional: apps_dim
    apps_dim_path = Path(args.raw) / "apps_dim.csv"
    if apps_dim_path.exists():
        print(f"[prepare] Writing apps_dim to {args.lake}/apps_dim.parquet")
        con.execute(APPS_DIM.format(raw=args.raw, lake=args.lake))

    build_rollups(con, args.lake, args.mvs)

    # Analyze tables (best-effort)
    print("[prepare] Analyzing tables...")
    for t in ["events", "mv_day_impr_revenue", "mv_day_country_publisher_impr",
              "mv_country_purchase_avg", "mv_adv_type_counts", "mv_day_minute_impr",
              "mv_hour_country_pub_adv_impr", "mv_day_country_type_adv", "mv_week_adv_country_type"]:
        try:
            con.execute(f"ANALYZE {t};")
        except duckdb.Error:
            pass

    # Optional: generalized MV family (grain x 1D) with guardrails
    try:
        build_mv_family(con, args.lake, args.mvs, max_rows=80_000_000, include_user=True)
    except Exception as e:
        print(f"[prepare] Warning: MV family build skipped due to: {e}")

    print(f"\n✓ Prepared Parquet lake: {args.lake}")
    print(f"✓ Materialized rollups: {args.mvs}")


if __name__ == "__main__":
    main()
