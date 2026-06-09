{{ config(
    schema = 'uniswap_v4_ethereum'
    , alias = 'aggregator_base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- swaps on Uniswap V4 BaseAggregatorHook pools: the hook routes the entire swap to an
-- external DEX and the pool holds no V4 liquidity, so these are aggregator trades
-- (uniswap v4 = the aggregator, the external DEX = the venue) and must not stay in
-- dex.trades next to the backing venue's row (double count)
with hook_swaps as (
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
        , call_trace_address as trace_address
    from {{ ref('uniswap_v4_ethereum_swaps') }}
    where is_aggregator_hook_swap
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
)

{{ add_tx_columns(model_cte = 'hook_swaps', blockchain = 'ethereum', columns = ['from', 'to']) }}
