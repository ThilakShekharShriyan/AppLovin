# Thilak: Query Planner Challenge

High-performance analytics on a single node with sub-second latency over an 11GB ad-events dataset. The system combines a materialized-view-first design with an adaptive query planner to deliver a 212x overall speedup while maintaining correctness and reliability.

## Architecture
- Data Lake: CSV → Parquet (ZSTD), day-partitioned for efficient pruning.
- Materialized Views (MVs): Narrow and wide “wide-format” MVs with filtered aggregates to cover common and evolving patterns.
- Adaptive Planner/Router:
  - MV-first routing with exact and fuzzy matching (re-aggregate when partially matched).
  - Health-aware fallback to partition-pruned base tables.
  - Optional 10% sampling for high-cardinality, ad‑hoc exploration.
- Safe Batch Runner: Memory-only timing, batch superset optimization, resource guards (≤20 queries, memory limits).
- Reliability Layer: Per-thread DuckDB connections, atomic staging→ready promotion, schema registry, MV integrity checks.
- Telemetry & Validation: 23-rule correctness suite (timezone, BETWEEN boundaries, AVG/SUM/COUNT parity, schema/data quality).

## Design Decisions (General)
- MV-First Strategy: Prefer pre-aggregation to avoid full scans; trade small storage for large speedups.
- Wide MVs + Filtered Aggregates: Increase coverage without explosion of MV count; enable fuzzy matching with exact correctness.
- Lazy, Columnar IO: Read directly from Parquet; no RAM-heavy table loading.
- Health-Aware Resilience: Route around rebuilding or unhealthy MVs; atomic files prevent corruption.
- Connection Safety: Per-thread DuckDB connections remove shared-heap contention.
- Simple Query Contract: JSON query spec keeps the interface stable and planner-driven.
- Externalized Raw Data: Keep repository light; reproducible local builds.

## Why This System Is Better
- Performance: 124.8s → 0.6s total; best-case single-query speedups up to 3,730x (vectorized DuckDB + MVs + pruning).
- Coverage: 20+ query patterns via narrow and wide MVs; 95%+ MV hit rate; graceful fallback ensures every query runs.
- Efficiency: ~200MB MV footprint (~1.8% of 11GB raw); memory-efficient lazy reads; single-node execution (CPU multi-threading).
- Reliability: Zero-crash concurrent operation; atomic writes; schema/mv consistency guarantees; 100% validation parity with base tables.
- Adaptivity: Dynamic MV suggestions from observed workload; optional sampling for the long tail of ad‑hoc analytics.

## What It Handles Well (Examples)
- Time-series aggregates: Daily/hourly revenue, minute-level impressions.
- Dimensional breakdowns: By advertiser, publisher, country, type.
- Complex filters: IN lists, BETWEEN date ranges, multi-dimensional GROUP BY.
- Batch workloads: Groups similar queries to compute once and project many.
- Fallback cases: High-cardinality or novel patterns via partition-pruned scans or controlled sampling.

## Quick Start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Build lake + MVs
python src/prepare.py --raw data/raw --lake data/lake --mvs data/mvs --threads 4 --mem 10GB

# Run adaptive engine
python src/runner_adaptive.py --lake data/lake --mvs data/mvs \
  --queries queries/examples --out data/outputs --threads 4 --mem 6GB --analyze
```

## Performance Summary
- Overall: 212x faster (baseline CSV scans vs. MV-first adaptive routing).
- Typical query latency: sub-second; planner overhead <1ms; 95%+ MV hit rate.

## Tech Stack
- Languages: Python, SQL (DuckDB), Bash/Make (tooling)
- Engine/Formats: DuckDB, Parquet (ZSTD), CSV, JSON
- Libraries: pyarrow/pandas, orjson
- Platform: Local single-node, CPU multi-threaded; no external cloud required

## Repository Pointers
- src/prepare.py — Build Parquet lake and materialized views
- src/runner_adaptive.py — Adaptive execution engine (planner + telemetry)
- src/planner_adaptive.py — Fuzzy MV matching, sampling, fallback logic
- docs/ARCHITECTURE.md — Deep dive on components and safety mechanisms
