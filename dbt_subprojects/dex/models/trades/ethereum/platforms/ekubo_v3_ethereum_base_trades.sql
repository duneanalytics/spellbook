{{
    config(
        schema = 'ekubo_v3_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

SELECT 
    'ethereum' AS blockchain
    , 'ekubo' AS project
    , '3' AS version
    , cast(date_trunc( 'month', t.block_time) as date) AS block_month
    , cast(date_trunc( 'day', t.block_time) as date) AS block_date
    , t.block_time AS block_time
    , t.block_number AS block_number
    , abs(case when t.amount0_raw < 0 then t.amount0_raw else t.amount1_raw end) as token_bought_amount_raw
    , abs(case when t.amount0_raw > 0 then t.amount0_raw else t.amount1_raw end) as token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , t.tx_from AS taker
    , t.id AS maker
    , 0x00000000000014aA86C5d3c41765bb24e11bd701 as project_contract_address
    , t.tx_hash
    , t.evt_index AS evt_index 
FROM {{ ref('ekubo_v3_ethereum_base_liquidity_events') }} t
WHERE event_type = 'swap'
AND {{ incremental_predicate('block_time') }}