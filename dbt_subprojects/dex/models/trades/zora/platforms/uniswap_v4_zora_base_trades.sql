{{ config(
    schema = 'uniswap_v4_zora'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

-- venue-side filter: swaps on BaseAggregatorHook pools are routed to an external DEX
-- and are reclassified into dex_aggregator.trades (see uniswap_v4_zora_aggregator_base_trades);
-- the heavy swap parsing lives in uniswap_v4_zora_swaps
select
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_amount_raw
        , token_sold_amount_raw
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , evt_index
        , sender
        , hooks
        , fee
        , liquidity
        , sqrtPriceX96
        , tick
        , call_trace_address
from {{ ref('uniswap_v4_zora_swaps') }}
where not is_aggregator_hook_swap
