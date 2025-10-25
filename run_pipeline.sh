#!/bin/bash
set -e

echo "=================================================="
echo "AppLovin Query Engine - Full Pipeline"
echo "=================================================="
echo ""

# Activate virtual environment
source .venv/bin/activate

# Check if data/raw exists and has files
if [ ! -d "data/raw" ] || [ -z "$(ls -A data/raw/events_part_*.csv 2>/dev/null)" ]; then
    echo "❌ ERROR: No CSV files found in data/raw/"
    echo "   Please place your events_part_*.csv files in data/raw/ first"
    exit 1
fi

echo "✓ Found $(ls data/raw/events_part_*.csv 2>/dev/null | wc -l | xargs) CSV files in data/raw/"
echo ""

# Step 1: Clean old outputs (keep lake/mvs if they exist)
echo "📦 Step 1/4: Cleaning old outputs..."
rm -rf data/outputs
mkdir -p data/outputs
echo "✓ Clean complete"
echo ""

# Step 2: Prepare data (build lake + MVs) - skip if already exists
if [ -d "data/lake/events" ] && [ -d "data/mvs" ]; then
    echo "🔨 Step 2/4: Lake and MVs already exist, skipping prepare..."
    echo "   (Delete data/lake and data/mvs to rebuild from scratch)"
else
    echo "🔨 Step 2/4: Building Parquet lake and materialized views..."
    echo "   ⏳ This takes ~5-10 minutes for large datasets (11GB CSV)..."
    python src/prepare_fast.py \
      --raw data/raw \
      --lake data/lake \
      --mvs data/mvs \
      --threads 4 \
      --mem 10GB
    echo "✓ Prepare complete"
fi
echo ""

# Step 3: Run optimized queries (with MVs)
echo "🚀 Step 3/4: Running queries with MV optimization..."
python src/runner.py \
  --lake data/lake \
  --mvs data/mvs \
  --queries queries/examples \
  --out data/outputs \
  --threads 4 \
  --mem 6GB
echo "✓ Optimized queries complete"
echo ""

# Step 4: Run baseline queries (no MVs)
echo "📊 Step 4/4: Running baseline queries (for comparison)..."
python src/baseline_main.py \
  --data-dir data/lake \
  --out-dir data/outputs/baseline \
  --queries queries/examples \
  --mode lake
echo "✓ Baseline queries complete"
echo ""

# Show results
echo "=================================================="
echo "✅ Pipeline Complete!"
echo "=================================================="
echo ""

# Calculate and display performance comparison
python << 'EOF'
import json
from pathlib import Path

opt = json.loads(Path('data/outputs/report.json').read_text())
base = json.loads(Path('data/outputs/baseline/baseline_report.json').read_text())

tot_opt = sum(q['seconds'] for q in opt['queries'])
tot_base = sum(q['seconds'] for q in base['queries'])
speedup = tot_base / tot_opt if tot_opt > 0 else 0

print("\n" + "="*60)
print(f"🚀 PERFORMANCE RESULTS")
print("="*60)
print(f"\n  Baseline (full scan):  {tot_base:.2f}s")
print(f"  Optimized (with MVs):  {tot_opt:.4f}s")
print(f"  Speedup:               {speedup:.1f}x faster")
print(f"  Time saved:            {tot_base - tot_opt:.2f}s ({(1 - tot_opt/tot_base)*100:.1f}% reduction)")

print("\n" + "="*60)
print("Per-Query Breakdown:")
print("="*60)

for i in range(len(opt['queries'])):
    q_name = opt['queries'][i]['query'].replace('.json', '')
    base_time = base['queries'][i]['seconds']
    opt_time = opt['queries'][i]['seconds']
    q_speedup = base_time / opt_time if opt_time > 0 else 0
    table = opt['queries'][i]['table']
    print(f"  {q_name:30s} {base_time:8.2f}s → {opt_time:8.4f}s  ({q_speedup:6.0f}x)  [{table}]")

print("\n" + "="*60)
print("\n📁 Output files:")
print("   • Optimized report: data/outputs/report.json")
print("   • Baseline report:  data/outputs/baseline/baseline_report.json")
print("   • CSV results:      data/outputs/*.csv")
print("")
EOF
