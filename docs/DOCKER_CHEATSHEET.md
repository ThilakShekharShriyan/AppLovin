# Docker Cheat Sheet - AppLovin Query Engine

## 🚀 Quick Commands

```bash
# Build image
make build

# Run adaptive system (default)
make run

# Run with Make alternatives
make run-original    # Original exact-match planner
make baseline        # No MVs, full scan
make prepare-wider   # Add 3 wider MVs only
```

---

## 📦 Docker Compose

```bash
# Run adaptive system
docker-compose run --rm runner-adaptive

# Run original system
docker-compose run --rm runner-original

# Add wider MVs
docker-compose run --rm prepare-wider

# Run baseline
docker-compose run --rm baseline
```

---

## 🐳 Raw Docker

```bash
# Build
docker build -t applovin-engine .

# Run adaptive
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
```

---

## 📊 View Results

```bash
# Adaptive report
cat data/outputs/report_adaptive.json

# MV suggestions
cat data/outputs/mv_suggestions.json

# With jq (pretty)
cat data/outputs/report_adaptive.json | jq '.summary'
```

---

## 🔧 Customize

```bash
# Different query directory
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/queries:/queries \
  applovin-engine \
  --lake /data/lake \
  --mvs /data/mvs \
  --queries /queries/custom \  # ← Change this
  --out /data/outputs \
  --threads 4 \
  --mem 4GB

# More memory
docker run --rm ... \
  --mem 8GB  # ← Change this

# More threads
docker run --rm ... \
  --threads 8  # ← Change this
```

---

## 🧹 Clean Up

```bash
# Remove containers
docker-compose down

# Remove image
docker rmi applovin-engine

# Prune everything
docker system prune -a

# Clean data
make clean
```

---

## ✅ What's Included

- ✅ Adaptive runner (fuzzy MV matching)
- ✅ Original runner (exact matching)
- ✅ Baseline runner (no MVs)
- ✅ Prepare script (build MVs)
- ✅ Prepare-wider script (add 3 MVs only)
- ✅ Pattern analyzer (MV suggestions)

---

## 📈 Performance

| System | Q1 | Q2 | Q3 | Q4 | Q5 | Total |
|--------|----|----|----|----|-------|-------|
| Adaptive (1 wide MV) | 1.9s ⚡ | 1.9s ⚡ | 59s 📊 | 62s 📊 | 7s 📊 | 133s |
| Original (0 MVs) | ~15s | ~15s | ~15s | ~15s | ~15s | ~75s |
| Baseline (no MVs) | ~15s | ~15s | ~15s | ~15s | ~15s | ~75s |

⚡ = Partial MV match  
📊 = Full scan

---

## 🎯 For Hackathon

```bash
# 1. Build once
make build

# 2. Demo adaptive system
make run

# 3. Show results
cat data/outputs/report_adaptive.json | jq '.queries[] | {query, match_type, seconds}'

# 4. Show MV suggestions
cat data/outputs/mv_suggestions.json | jq '.suggestions'
```

**Key Points:**
- ✅ Containerized = Reproducible
- ✅ Works with limited MVs
- ✅ Self-optimizing (suggests new MVs)
- ✅ 40% hit rate with 1 wide MV
