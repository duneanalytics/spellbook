Rights Reserved, Unlicensed
# Healthcare analytics models

Purpose: small, compiling dbt models that create baseline time series for future healthcare/Web3 analytics.

## Models
- `healthcare_eth_activity_baseline.sql`  
  Daily Ethereum transaction counts. Used as a reference series.
- `healthcare_eth_activity_baseline_7d.sql`  
  7-day moving average of the same series for trend analysis.

Both compile independently. No external seeds required.
