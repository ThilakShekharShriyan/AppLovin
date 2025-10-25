# AppLovin Query Engine - Materialized View Optimizer

A high-performance query engine that accelerates aggregate queries on ad-event data using pre-computed materialized views (MVs) and intelligent query planning.

## Architecture

```
Raw CSV → prepare.py → Parquet lake (partitioned by day)
                    → Materialized rollups (MVs) in Parquet ┐
                                                            ├─ runner.py → choose MV or fallback → outputs/*.csv + report.json
Parquet lake (events_v) ────────────────────────────────────┘
```

## How It Works

**Preparation Phase** (`prepare.py`):
1. Ingests raw CSV events with proper type casting and timestamp derivation
2. Writes partitioned Parquet lake (by day) for efficient scanning
3. Builds 5 specialized materialized view rollups:
   - `mv_day_impr_revenue` - daily impression revenue
   - `mv_day_country_publisher_impr` - publisher revenue by country/day
   - `mv_country_purchase_avg` - average purchase price by country
   - `mv_adv_type_counts` - event counts per advertiser/type
   - `mv_day_minute_impr` - minute-grain impression spend

**Execution Phase** (`runner.py`):
1. Parses JSON queries from the queries/ directory
2. Uses intelligent planner to route queries to optimal MV or fallback to full scan
3. Executes queries on in-memory MV tables with DuckDB
4. Outputs CSV results + performance report

**Why It's Fast**:
- **Tiny scans**: MVs are 100-1000x smaller than raw events
- **Day partition pruning**: Only reads relevant date ranges from Parquet
- **Columnar compression**: ZSTD-compressed Parquet minimizes I/O
- **In-memory execution**: MVs loaded into DuckDB tables for sub-millisecond queries
- **Deterministic routing**: Zero overhead query rewriting based on JSON structure

## Quick Start

### Docker (Recommended)

```bash
# Build image
make build

# Prepare data (untimed)
make prepare

# Run queries (timed)
make run

# Optional: Run baseline for comparison
make baseline
```

### Local Python

```bash
# Install dependencies
pip install -r requirements.txt

# Prepare data
python src/prepare.py --raw data/raw --lake data/lake --mvs data/mvs

# Run queries
python src/runner.py \
  --lake data/lake \
  --mvs data/mvs \
  --queries queries \
  --out data/outputs \
  --threads 8 \
  --mem 6GB
```

## Resource Constraints

- **RAM**: ≤16GB (configurable via `--mem`)
- **Disk**: ≤100GB (ZSTD compression keeps MVs compact)
- **Network**: None required (local execution)

## Query Format

Queries are JSON files in the `queries/` directory:

```json
{
  "from": "events",
  "select": [
    "day",
    {"SUM": "bid_price"}
  ],
  "where": [
    {"col": "type", "op": "eq", "val": "impression"},
    {"col": "day", "op": "between", "val": ["2025-01-01", "2025-01-31"]}
  ],
  "group_by": ["day"]
}
```

## Performance Report

After execution, see `data/outputs/report.json`:

```json
{
  "queries": [
    {
      "query": "q1_daily_revenue.json",
      "table": "mv_day_impr_revenue",
      "seconds": 0.0042,
      "output": "data/outputs/q1_daily_revenue.csv"
    }
  ]
}
```

## Project Structure

```
applovin-query-engine/
├── README.md
├── requirements.txt
├── Makefile
├── Dockerfile
├── data/
│   ├── raw/              # Input: events.csv, apps_dim.csv
│   ├── lake/             # Generated: Parquet partitioned by day
│   ├── mvs/              # Generated: Materialized view rollups
│   └── outputs/          # Results: CSV files + report.json
├── queries/              # JSON query files (*.json)
│   └── examples/         # Example queries
└── src/
    ├── prepare.py        # Build lake + MVs
    ├── runner.py         # Execute JSON queries (optimized)
    ├── planner.py        # JSON → MV selection logic
    ├── assembler.py      # JSON → SQL translator (baseline)
    ├── baseline_main.py  # Baseline runner (no MVs)
    └── validate.py       # Correctness validation
```

## Validation

To verify MV correctness against full scans:

```bash
python src/validate.py --lake data/lake --mvs data/mvs --queries queries
```

## Design Decisions

1. **Parquet over CSV**: 10-50x faster scans due to columnar layout and compression
2. **Day partitioning**: Enables partition pruning for date-filtered queries
3. **In-memory MVs**: Sub-millisecond query latency vs disk I/O overhead
4. **Static rollups**: Deterministic plan selection avoids optimizer overhead
5. **Fallback safety**: Always falls back to full scan if MV doesn't match

## Benchmarking

Compare optimized vs baseline:

```bash
# Run both
make prepare
make run
make baseline

# Compare reports
diff data/outputs/report.json data/outputs/baseline_report.json
```

Expected speedup: **10-100x** for aggregate queries matching MV patterns.
