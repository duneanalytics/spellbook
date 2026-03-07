# 1inch Main Lineage Overview

## Streams:
- **ar** - classic swaps (aggregation router)
- **lo** - limit swaps (limit order protocol)
- **cc** - cross-chain swaps

## Model layers:
1. **raw_calls** - 1inch calls data from `traces` models
  accessing:
    - `oneinch_{blockchain}.{stream}_raw_calls` - _materialized viewes_
    - `oneinch_evms.raw_calls` - _view_ - unioned view of all exposed blockchains
2. **decoded** (`ar` | `lo` | `cc`) -  from `decoded tables` and `raw_calls`
  accessing:
    - `oneinch_{blockchain}.{stream}` - _materialized viewes_
    - `oneinch.{stream}` - _view_ - unioned view of all exposed blockchains
3. **transfers** - all transfers within transactions in which called 1inch contracts (data from `raw_calls` and `transfers_from_traces`)
  accessing:
    - `oneinch_{blockchain}.transfers` - _materialized viewes_ - need all streams for this; each transfer is related to a specific call within the transaction; there may be calls from different streams within a transaction
    - `oneinch_evms.transfers` - _view_ - unioned view of all exposed blockchains
4. **executions** - merge `decoded` and `transfers` to get what the user actually gave and received as a result of calling the 1inch contract
  accessing:
    - `oneinch_{blockchain}.{stream}_executions` - _materialized viewes_
    - `oneinch.ar_executions` - _view_ - unioned view of all exposed blockchains
    - `oneinch.lo_executions` - _view_ - unioned view of all exposed blockchains
    - `oneinch.cc_executions` - _materialized view_ - unioned view of all exposed blockchains with a matched sources and destinations
5. **swaps** - aggregate `executions` - _materialized view_ - a top-level data mart containing aggregated data on user exchanges
  accessing: `oneinch.swaps` - _materialized view_ - combining and allocating modes & limits second side

## Configs:
- **meta_cfg** - a config that aggregated meta data and settings of streams, blockchains etc.
- **{stream}_contracts_cfg** - a config for stream contracts