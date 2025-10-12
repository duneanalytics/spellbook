# 1inch Main Lineage Overview

## Streams:
- **ar** - classic swaps (aggregation router)
- **lo** - limit swaps (limit order protocol)
- **cc** - cross-chain swaps

## Model layers:
1. **raw_calls** - 1inch calls data from `traces` models
2. **decoded** (`ar` | `lo` | `cc`) -  from `decoded tables` and `raw_calls`
3. **raw_transfers** - all transfers within transactions in which called 1inch contracts (data from `raw_calls` and `transfers_from_traces`)
4. **executions** - merge `decoded` and `raw_transfers` to get what the user actually gave and received as a result of calling the 1inch contract
5. **swaps** - aggregate `executions` - a top-level data mart containing aggregated data on user exchanges

## Configs:
- **meta_cfg** - a config that aggregated meta data and settings of streams, blockchains etc.
- **{stream}_cfg** - a config for stream contracts