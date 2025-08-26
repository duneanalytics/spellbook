# Prices Minute Interpolation Testing

## Overview

This directory contains test models and validation for the sparse hour interpolation approach used to build `prices.minute` from `prices.hour` data.

## Sparse Hour Interpolation Approach

### Problem Statement

The traditional `prices.minute` table can suffer from:
- **High noise and outliers** from minute-by-minute DEX trade aggregation
- **Data quality issues** due to sandwich attacks, MEV, and anomalous trades
- **Performance challenges** due to dense minute-level data storage
- **Inconsistent pricing** during low-liquidity periods

### Solution: Sparse Hour Interpolation

Instead of aggregating prices at the minute level directly from DEX trades, this approach:

1. **Uses hourly aggregated prices as anchor points** - These are more stable and have outlier filtering
2. **Forward-fills minute data within each hour** - Maintains temporal resolution without noise
3. **Handles missing hours gracefully** - Continues forward-fill from last available hour
4. **Provides consistent UX** - Complete minute-level timeseries for applications

### Implementation Details

#### Key Components

1. **Source Data**: `prices.hour` table with clean, aggregated hourly prices
2. **Time Series Generation**: Creates minute timestamps from hour boundaries  
3. **Forward Fill Logic**: Applies hourly price to all minutes within that hour
4. **Gap Handling**: Manages missing hourly data points appropriately

#### SQL Logic Flow

```sql
WITH hourly_prices AS (
    -- Get sparse hourly anchor points with next hour lookup
    SELECT *, lead(timestamp) OVER (...) as next_hour
    FROM prices.hour
),

minute_timeseries AS (
    -- Generate 60 minutes for each hour, stopping at next actual data point
    SELECT hp.*, date_add('minute', seq, hp.timestamp) as minute
    FROM hourly_prices hp
    CROSS JOIN unnest(sequence(0, 59)) as seq
    WHERE minute < coalesce(next_hour, timestamp + interval '1' hour)
)

SELECT blockchain, contract_address, symbol, decimals, minute, price
FROM minute_timeseries
```

#### Benefits

- **Reduced Outliers**: Hourly aggregation filters out minute-level noise
- **Better Performance**: Less dense data, faster queries  
- **Data Consistency**: Stable prices within hour boundaries
- **Maintainability**: Simpler logic than complex minute-level outlier detection

## Testing Framework

### Test Models

- `prices_minute_interpolated_test.sql` - Validation model with sample scenarios
- Test cases include normal hours, missing hours, and edge cases

### Validation Checks

1. **Completeness**: All expected minutes are generated
2. **Consistency**: Price remains constant within each hour
3. **Gap Handling**: Missing hours don't break the timeseries
4. **Performance**: Query execution time within acceptable bounds

### Running Tests

```bash
# Run the test model
dbt run --models prices_minute_interpolated_test

# Check validation results
dbt test --models prices_minute_interpolated
```

## Usage in Production

### Migration Strategy

1. **Phase 1**: Deploy interpolated model alongside existing minute table
2. **Phase 2**: A/B test with downstream applications (tokens.transfers, etc.)
3. **Phase 3**: Gradually migrate consumers to interpolated version
4. **Phase 4**: Deprecate original dense minute table

### Performance Considerations

- **Incremental Loading**: Uses 3-day lookback for incremental builds
- **Partitioning**: Partitioned by blockchain for query efficiency  
- **Indexing**: Optimized for time-series access patterns

### Monitoring

Key metrics to track:
- Data freshness (lag from hourly to minute availability)
- Query performance improvements vs original table
- Data quality (price stability, no gaps)
- Downstream application compatibility

## References

- Linear Issue: CUR2-465 - Build prices.minute as interpolated version of prices.hour
- Related PRD: [Prices V3](https://www.notion.so/duneanalytics/Prices-V3-214d8bb32548802b9559d1600570865f)
- Original Discussion: [GitHub Issue 8095](https://github.com/duneanalytics/spellbook/issues/8095)