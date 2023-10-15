{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v3_ethereum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2021-05-04' %}

WITH dexs AS
(
    --Uniswap v3
    SELECT t.evt_block_time                                                AS block_time
         , t.recipient                                                     AS taker
         , CAST(NULL as VARBINARY) as maker
         , CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
         , CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
         , CASE WHEN amount0 < INT256 '0' THEN f.token0 ELSE f.token1 END AS token_bought_address
         , CASE WHEN amount0 < INT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
         , t.contract_address as project_contract_address
         , f.fee
         , t.evt_tx_hash AS tx_hash
         , t.evt_index
    FROM {{ source('uniswap_v3_ethereum', 'Pair_evt_Swap') }} t
    INNER JOIN {{ source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated') }} f
        ON f.pool = t.contract_address
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}
)

SELECT 
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , dexs.block_time
    , CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw
    , CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM dexs