{{ config(
    schema = 'uniswap_v4'
    , alias = 'liquidity_events'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'event_type']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'[
                                      "ethereum","arbitrum","base","ink","blast","optimism","blast","bnb","zora","avalanche_c","unichain","worldchain"
                                    ]\',
                                    "project",
                                    "uniswap",
                                    \'["Henrystats"]\') }}')
}}

WITH 

v4_liquidity_events as (
    select 
        * 
    from 
    {{ ref('uniswap_v4_base_liquidity_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
),

uniswap_liquidity_events as (
    select 
        * 
    from 
    {{ ref('uniswap_liquidity_events') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

          SELECT
                 v4.blockchain
                , v4.project
                , v4.version
                , v4.block_month
                , v4.block_date
                , v4.block_time
                , v4.block_number
                , v4.id
                , v4.tx_hash
                , v4.x_from
                , v4.evt_index
                , v4.event_type
                , v4.token0
                , v4.token1
                , v4.token0_symbol 
                , token1_symbol
                , v4.amount0_raw
                , v4.amount1_raw
                , ul.amount0
                , ul.amount1
                , ulamount0_usd
                , ul.amount1_usd
                , v4.liquidityDelta
                , v4.sqrtPriceX96
                , v4.tickLower
                , v4.tickUpper
                , v4.salt
           FROM
           v4_liquidity_events v4 
           LEFT JOIN 
           uniswap_liquidity_events ul 
            ON v4.tx_hash = ul.tx_hash 
            AND v4.id = ul.id 
            AND v4.evt_index = ul.evt_index 
            AND v4.evevnt_type = ul.event_type 