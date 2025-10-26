#!/usr/bin/env python3
"""
Build ALL materialized views (original 5 + wider 3) from an existing Parquet lake.
Skips rebuilding the lake; reads from lake and writes partitioned Parquet MVs.
"""
import argparse
import duckdb
from pathlib import Path
from prepare import build_rollups


def main():
    ap = argparse.ArgumentParser(description="Build all MVs from existing lake")
    ap.add_argument("--lake", required=True, help="Path to existing Parquet lake")
    ap.add_argument("--mvs", required=True, help="Output path for materialized views")
    ap.add_argument("--threads", type=int, default=4, help="DuckDB thread count")
    ap.add_argument("--mem", default="6GB", help="DuckDB memory limit")
    args = ap.parse_args()

    Path(args.mvs).mkdir(parents=True, exist_ok=True)

    print(f"[mvs-only] Connecting to DuckDB (threads={args.threads}, mem={args.mem})")
    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={args.threads};")
    con.execute(f"SET memory_limit='{args.mem}';")

    build_rollups(con, args.lake, args.mvs)

    print(f"\n✓ Built all MVs to {args.mvs}")


if __name__ == "__main__":
    main()
