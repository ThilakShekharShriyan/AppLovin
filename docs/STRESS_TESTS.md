# Advanced Stress Test Suite

This document describes the 10 complex analytical queries designed to test the robustness and performance of the MV-first optimization system.

## 🎯 Test Objectives

1. **Multi-dimensional Grouping**: Validate performance on 2D and 3D GROUP BY operations
2. **Complex Filtering**: Test IN operators, date ranges, and multi-column WHERE clauses  
3. **Time Grain Variations**: Exercise day/hour/week granularity in different combinations
4. **Fallback Behavior**: Ensure graceful degradation when MVs don't match
5. **Batch Optimization**: Verify efficient handling of similar query patterns

## 📋 Stress Test Queries

### 🧩 S1: Revenue by Country and Day
```json
{
  "from": "events",
  "select": ["country", "day", {"SUM": "bid_price"}],
  "where": [{"col": "type", "op": "eq", "val": "impression"}],
  "group_by": ["country", "day"],
  "order_by": [{"col": "day", "dir": "asc"}]
}
```
**Tests**: 2D grouping (country × day) with ordering  
**Expected**: Routes to `mv_day_country_wide`  
**Result**: ✅ 0.145s, 4,384 combinations

### 🧠 S2: Click-Through Rate per Advertiser
```json
{
  "from": "events",
  "select": ["advertiser_id", {"COUNT": "*"}],
  "where": [{"col": "type", "op": "in", "val": ["click", "impression"]}],
  "group_by": ["advertiser_id"],
  "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
}
```
**Tests**: IN operator filtering with multiple event types  
**Expected**: Falls back to `events_v` (no single MV covers both types)

### 💰 S3: Top Advertisers by Purchase Value (US Q4 2024)
```json
{
  "from": "events",
  "select": ["advertiser_id", {"SUM": "total_price"}],
  "where": [
    {"col": "type", "op": "eq", "val": "purchase"},
    {"col": "country", "op": "eq", "val": "US"},
    {"col": "day", "op": "between", "val": ["2024-10-01", "2024-12-30"]}
  ],
  "group_by": ["advertiser_id"],
  "order_by": [{"col": "SUM(total_price)", "dir": "desc"}]
}
```
**Tests**: Complex WHERE with date range + country + type filtering  
**Expected**: `mv_day_advertiser_id_wide` with partition pruning

### 📆 S4: Average Bid Price per Publisher per Week
```json
{
  "from": "events",
  "select": ["publisher_id", "week", {"AVG": "bid_price"}],
  "where": [{"col": "type", "op": "eq", "val": "impression"}],
  "group_by": ["publisher_id", "week"],
  "order_by": [{"col": "AVG(bid_price)", "dir": "desc"}]
}
```
**Tests**: Weekly time grain with 2D grouping  
**Expected**: `mv_week_publisher_id_wide`

### ⏰ S5: Hourly Spend Pattern (Japan, Single Day)
```json
{
  "from": "events",
  "select": ["hour", {"SUM": "bid_price"}],
  "where": [
    {"col": "type", "op": "eq", "val": "impression"},
    {"col": "country", "op": "eq", "val": "JP"},
    {"col": "day", "op": "eq", "val": "2024-11-11"}
  ],
  "group_by": ["hour"],
  "order_by": [{"col": "hour", "dir": "asc"}]
}
```
**Tests**: Hourly precision with tight day + country filtering  
**Expected**: Partition-aware `events_v` or `mv_hour_*`

### 🌍 S6: Total Impressions per Country (All Time)
```json
{
  "from": "events",
  "select": ["country", {"COUNT": "*"}],
  "where": [{"col": "type", "op": "eq", "val": "impression"}],
  "group_by": ["country"],
  "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
}
```
**Tests**: Global aggregation across all time periods  
**Expected**: `mv_day_country_wide`  
**Result**: ✅ 0.189s, 13 countries

### 💡 S7: Advertiser-Publisher Matrix (Clicks, Jan 2024)
```json
{
  "from": "events",
  "select": ["advertiser_id", "publisher_id", {"COUNT": "*"}],
  "where": [
    {"col": "type", "op": "eq", "val": "click"},
    {"col": "day", "op": "between", "val": ["2024-01-01", "2024-01-31"]}
  ],
  "group_by": ["advertiser_id", "publisher_id"]
}
```
**Tests**: 2D cross-dimensional matrix with date filtering  
**Expected**: Falls back to partition-aware `events_v`

### 🛒 S8: Purchase Revenue (Germany + France)
```json
{
  "from": "events",
  "select": ["day", {"SUM": "total_price"}],
  "where": [
    {"col": "type", "op": "eq", "val": "purchase"},
    {"col": "country", "op": "in", "val": ["DE", "FR"]}
  ],
  "group_by": ["day"],
  "order_by": [{"col": "day", "dir": "asc"}]
}
```
**Tests**: Multi-country IN filtering with purchase aggregation  
**Expected**: `mv_day_country_wide` with multi-value filtering

### ⚙️ S9: High-Traffic Days (July 2024)
```json
{
  "from": "events",
  "select": ["day", {"COUNT": "*"}],
  "where": [
    {"col": "type", "op": "eq", "val": "impression"},
    {"col": "day", "op": "between", "val": ["2024-07-01", "2024-07-07"]}
  ],
  "group_by": ["day"],
  "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
}
```
**Tests**: Date range filtering with impression counts  
**Expected**: `mv_day_type_wide` with partition pruning

### 🪙 S10: Average Purchase Value (Advertiser × Country)
```json
{
  "from": "events",
  "select": ["advertiser_id", "country", {"AVG": "total_price"}],
  "where": [{"col": "type", "op": "eq", "val": "purchase"}],
  "group_by": ["advertiser_id", "country"]
}
```
**Tests**: Maximum 3D grouping complexity  
**Expected**: Falls back to `events_v` (no 3D MV available)

## 🚀 Performance Expectations

**MV Hits** (Sub-second):
- S1, S3, S4, S6, S8, S9: Should route to appropriate wide MVs
- Expected times: 0.1-0.5 seconds

**Fallbacks** (Still fast due to partition pruning):
- S2, S5, S7, S10: Route to partition-aware `events_v`
- Expected times: 1-10 seconds (vs 20-30s baseline)

## 📊 Running the Tests

```bash
# Run all stress tests
python src/runner.py --lake data/lake --mvs data/mvs --queries queries/stress_test --out data/outputs/stress_test

# Run safe subset (no date issues)
python src/runner.py --lake data/lake --mvs data/mvs --queries queries/demo_stress --out data/outputs/demo_stress

# View results
cat data/outputs/stress_test/report.json
```

This comprehensive test suite validates that the MV-first architecture can handle complex analytical workloads while maintaining excellent performance.