{{
    config(
        schema = 'oneinch',
        alias = 'lop_aggregator_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'block_month', 'tx_hash', 'trace_address', 'evt_index'],
    )
}}

-- venue-settled LOP fills: the underlying venue's own row stays in dex.trades, so the
-- intent-layer fill is recorded in dex_aggregator.trades instead, via the base tier
-- (dex_aggregator_base_trades; symbols/amounts/usd are enriched downstream like every
-- other base trades model). User perspective, like oneinch_ar_trades.
-- evt_index comes persisted from oneinch_lop_venue_settled_fills (same numbering as
-- oneinch_lop_own_trades via oneinch_lop_evt_index()); keeping this view window-free
-- lets consumers' block_time predicates push down into the oneinch_swaps scan
select
    f.blockchain
    , '1inch-LOP' as project
    , cast(f.protocol_version as varchar) as version
    , f.block_date
    , f.block_month
    , f.block_time
    , f.dst_executed_amount as token_bought_amount_raw
    , f.src_executed_amount as token_sold_amount_raw
    , f.dst_token_address as token_bought_address
    , f.src_token_address as token_sold_address
    , f.user as taker
    , cast(null as varbinary) as maker
    , f.call_to as project_contract_address
    , f.tx_hash
    , f.tx_from
    , f.tx_to
    , f.call_trace_address as trace_address
    , v.evt_index
from {{ ref('oneinch_swaps') }} as f
join {{ ref('oneinch_lop_venue_settled_fills') }} as v
    on v.blockchain = f.blockchain
    and v.block_month = f.block_month
    and v.execution_id = f.execution_id
where true
    and f.mode = 'limits'
    and f.tx_success
    and f.call_success
