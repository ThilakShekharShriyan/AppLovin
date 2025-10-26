# Quick Command Reference

## Full Pipeline (Recommended)

Run everything in one go:

```bash
./run_pipeline.sh
```

This will:
1. ✓ Check for CSV files in `data/raw/`
2. 🔨 Build Parquet lake + MVs (or skip if already built)
3. 🚀 Run optimized queries with MVs
4. 📊 Run baseline queries for comparison
5. 📈 Show performance results

---

## Individual Commands

### 1. Setup (one time)

```bash
# Install dependencies
pip install -r requirements.txt

# Or use virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Prepare Data (build lake + MVs)

**⚠️ Takes 5-10 minutes for large datasets**

```bash
python src/prepare_fast.py \
  --raw data/raw \
  --lake data/lake \
  --mvs data/mvs \
  --threads 4 \
  --mem 10GB
```

### 3. Run Queries (Optimized with MVs)

```bash
python src/runner.py \
  --lake data/lake \
  --mvs data/mvs \
  --queries queries/examples \
  --out data/outputs \
  --threads 4 \
  --mem 6GB
```

### 4. Run Baseline (No MVs)

```bash
python src/baseline_main.py \
  --data-dir data/lake \
  --out-dir data/outputs/baseline \
  --queries queries/examples \
  --mode lake
```

### 5. View Results

```bash
# View optimized results
cat data/outputs/report.json

# View baseline results
cat data/outputs/baseline/baseline_report.json

# View CSV output
ls -lh data/outputs/*.csv
head data/outputs/q1_daily_impr_revenue.csv
```

---

## Quick Performance Check

```bash
python << 'EOF'
import json
from pathlib import Path

opt = json.loads(Path('data/outputs/report.json').read_text())
base = json.loads(Path('data/outputs/baseline/baseline_report.json').read_text())

tot_opt = sum(q['seconds'] for q in opt['queries'])
tot_base = sum(q['seconds'] for q in base['queries'])

print(f"\n🚀 SPEEDUP: {tot_base/tot_opt:.1f}x faster")
print(f"   {tot_base:.2f}s → {tot_opt:.4f}s\n")
EOF
```

---

## Troubleshooting

### Out of Memory Error

Increase memory limit:
```bash
python src/prepare_fast.py --raw data/raw --lake data/lake --mvs data/mvs --mem 12GB
```

### Rebuild Everything

```bash
rm -rf data/lake data/mvs data/outputs
./run_pipeline.sh
```

### Skip Prepare (use existing lake/mvs)

The script automatically skips prepare if `data/lake/events` and `data/mvs` exist.

To force rebuild:
```bash
rm -rf data/lake data/mvs
./run_pipeline.sh
```

---

## Docker (Alternative)

Build and run in Docker:

```bash
# Build image
make build

# Prepare data
make prepare

# Run queries
make run

# Run baseline
make baseline
```

---

## File Structure

```
AppLovin/
├── data/
│   ├── raw/              # Input: events_part_*.csv files (put your data here)
│   ├── lake/             # Generated: Parquet lake
│   ├── mvs/              # Generated: Materialized views
│   └── outputs/          # Generated: Query results
│       ├── *.csv         # Optimized query results
│       ├── report.json   # Optimized performance report
│       └── baseline/
│           ├── *.csv             # Baseline query results
│           └── baseline_report.json
├── queries/
│   └── examples/         # JSON query files (*.json)
├── src/                  # Python source code
├── run_pipeline.sh       # Main execution script
└── README.md             # Project documentation
```

---

## Add Your Own Queries

1. Create a JSON file in `queries/examples/`:

```json
{
  "from": "events",
  "select": [
    "day",
    {"SUM": "bid_price"}
  ],
  "where": [
    {"col": "type", "op": "eq", "val": "impression"}
  ],
  "group_by": ["day"]
}
```

2. Run the pipeline:
```bash
./run_pipeline.sh
```

Your query will be automatically optimized if it matches an MV pattern!
