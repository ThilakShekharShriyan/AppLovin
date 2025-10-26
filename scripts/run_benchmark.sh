#!/usr/bin/env bash
set -euo pipefail

# Run end-to-end comparison across DuckDB (MV), DuckDB (full scan), and TimescaleDB
# Usage:
#   LAKE=./data/lake \
#   MVS=./data/mvs \
#   QUERIES=./queries/examples \
#   OUT=./out/bench_$(date +%Y%m%d_%H%M%S) \
#   scripts/run_benchmark.sh
#
# Required env vars:
#   LAKE, MVS, QUERIES, OUT

: "${LAKE:?Set LAKE path to Parquet lake}"
: "${MVS:?Set MVS path to MV Parquet dir}"
: "${QUERIES:?Set QUERIES path to JSON queries dir}"
: "${OUT:?Set OUT output dir}"

mkdir -p "$OUT"

echo "[run] Writing outputs to: $OUT"
python3 src/benchmark_comparison.py \
  --lake "$LAKE" \
  --mvs "$MVS" \
  --queries "$QUERIES" \
  --out "$OUT"

echo "[run] Done. Reports:"
echo "  - $OUT/benchmark_comparison.json"
echo "  - $OUT/benchmark_comparison.md"
