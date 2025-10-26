# Docker Setup

## Quick Start

### Option 1: Using Make (Recommended)

```bash
# Build image
make build

# Add wider MVs to existing lake
make prepare-wider

# Run adaptive system
make run

# Run original system (for comparison)
make run-original

# Run baseline (no MVs)
make baseline
```

---

### Option 2: Using Docker Compose

```bash
# Build image
docker-compose build

# Add wider MVs only (fast - uses existing lake)
docker-compose run --rm prepare-wider

# Run adaptive system
docker-compose run --rm runner-adaptive

# Run original system
docker-compose run --rm runner-original

# Run baseline
docker-compose run --rm baseline

# Full prepare (rebuild everything - slow)
docker-compose run --rm prepare
```

---

### Option 3: Using Docker CLI

```bash
# Build image
docker build -t applovin-engine .

# Add wider MVs only
docker run --rm \
  -v $(pwd)/data:/data \
  --entrypoint python \
  applovin-engine /app/prepare_wider_mvs_only.py \
  --lake /data/lake \
  --mvs /data/mvs \
  --threads 4 \
  --mem 6GB

# Run adaptive system
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/queries:/queries \
  applovin-engine \
  --lake /data/lake \
  --mvs /data/mvs \
  --queries /queries/examples \
  --out /data/outputs \
  --threads 4 \
  --mem 4GB \
  --analyze

# Run original system
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/queries:/queries \
  --entrypoint python \
  applovin-engine /app/runner.py \
  --lake /data/lake \
  --mvs /data/mvs \
  --queries /queries/examples \
  --out /data/outputs \
  --threads 4

# Run baseline
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/queries:/queries \
  --entrypoint python \
  applovin-engine /app/baseline_main.py \
  --data-dir /data/lake \
  --out-dir /data/outputs/baseline \
  --queries /queries/examples \
  --mode lake
```

---

## What Each Command Does

### `make build` / `docker-compose build`
Builds the Docker image with:
- Python 3.11
- All dependencies (duckdb, pandas, orjson, etc.)
- All source files from `src/`

### `make prepare-wider`
**Use this if you already have the lake built**
- Only builds the 3 wider MVs
- Faster (5-10 min vs 15-20 min)
- Requires less disk space

### `make prepare`
**Full rebuild - use only if needed**
- Rebuilds lake from raw CSV
- Builds all 8 MVs (5 + 3 wide)
- Takes 15-20 minutes

### `make run` (Adaptive)
- Uses adaptive planner
- Fuzzy MV matching
- Pattern tracking
- Generates `report_adaptive.json` + `mv_suggestions.json`

### `make run-original`
- Uses original exact-match planner
- Only matches narrow MVs
- Generates `report.json`

### `make baseline`
- No MVs, full scan only
- For performance comparison
- Generates `baseline_report.json`

---

## Typical Workflow

### For Hackathon Demo

```bash
# 1. Build image (one time)
make build

# 2. Add wider MVs (if you have existing lake)
make prepare-wider

# 3. Run adaptive system
make run

# 4. View results
cat data/outputs/report_adaptive.json
cat data/outputs/mv_suggestions.json

# 5. Optional: Compare with original
make run-original
diff data/outputs/report.json data/outputs/report_adaptive.json
```

---

## Resource Configuration

### Memory Limits
Edit `docker-compose.yml` or Makefile:
```yaml
--mem 4GB  # Change to 2GB, 6GB, 8GB, etc.
```

### Thread Count
```yaml
--threads 4  # Change to 2, 8, 16, etc.
```

### Query Directory
```yaml
--queries /queries/examples  # Or /queries/custom
```

---

## Troubleshooting

### Out of Memory
```bash
# Reduce memory limit
docker-compose run --rm runner-adaptive \
  --lake /data/lake \
  --mvs /data/mvs \
  --queries /queries/examples \
  --out /data/outputs \
  --threads 2 \
  --mem 2GB
```

### Out of Disk Space
```bash
# Clean Docker cache
docker system prune -a

# Or use prepare-wider instead of prepare
make prepare-wider
```

### Container Won't Start
```bash
# Check logs
docker-compose logs runner-adaptive

# Rebuild image
docker-compose build --no-cache
```

### Permission Issues
```bash
# Fix data directory permissions
sudo chown -R $USER:$USER data/
```

---

## Volumes

The container mounts:
- `./data` → `/data` (input/output data)
- `./queries` → `/queries` (JSON query files)

Results are written to `./data/outputs/` on your host machine.

---

## Environment Variables

Set in `docker-compose.yml`:
```yaml
environment:
  - PYTHONUNBUFFERED=1  # Real-time output
  - DUCKDB_THREADS=4    # Optional
  - DUCKDB_MEMORY=4GB   # Optional
```

---

## Performance Tips

1. **Use prepare-wider** if lake exists (much faster)
2. **Adjust --mem** based on available RAM
3. **Mount data on SSD** for faster I/O
4. **Use --threads** = number of CPU cores
5. **Run baseline separately** (it's slow)

---

## Example: Full Comparison

```bash
# Build everything
make build
make prepare-wider

# Run all three systems
make run-adaptive      # Adaptive with fuzzy matching
make run-original      # Original exact matching
make baseline          # No MVs (full scan)

# Compare results
echo "=== Adaptive ==="
cat data/outputs/report_adaptive.json | jq '.summary'

echo "=== Original ==="
cat data/outputs/report.json | jq '.queries[].seconds'

echo "=== Baseline ==="
cat data/outputs/baseline/baseline_report.json | jq '.queries[].seconds'
```

---

## Size Information

- **Image size**: ~500MB
- **Lake size**: ~4.6GB
- **MVs size**: ~60MB (narrow) + ~540MB (wide) = 600MB
- **Total**: ~5.7GB

---

## Clean Up

```bash
# Remove containers
docker-compose down

# Remove image
docker rmi applovin-engine

# Remove all Docker cache
docker system prune -a

# Remove data (⚠️ destructive)
make clean
```
