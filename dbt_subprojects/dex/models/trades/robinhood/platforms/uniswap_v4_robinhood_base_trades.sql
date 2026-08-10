{{ config(
    schema = 'uniswap_v4_robinhood'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

-- heavy swap parsing lives in uniswap_v4_robinhood_swaps; no aggregator-hook routing on robinhood yet,
-- so this is a plain passthrough (see uniswap_v4_base_base_trades for the aggregator-hook-filtered variant)
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
from {{ ref('uniswap_v4_robinhood_swaps') }}
