{{ config(
    tags=['dunesql'],
    schema = 'defiswap_ethereum',
    alias ='base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2020-09-09' %}

WITH dexs AS
(
    -- defiswap
    SELECT
        t.evt_block_time AS block_time
        ,t.to AS taker
        ,t.contract_address AS maker
        ,CAST(NULL AS DOUBLE) AS amount_usd
        ,CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        ,CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        ,CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address
        ,CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        ,t.contract_address AS project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,t.evt_index AS evt_index
    FROM {{ source('defiswap_ethereum', 'CroDefiSwapPair_evt_Swap') }} t
    INNER JOIN {{ source('crodefi_ethereum', 'CroDefiSwapFactory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

SELECT TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
     ,CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
     , dexs.block_time
     , dexs.token_bought_amount_raw AS token_bought_amount_raw
     , dexs.token_sold_amount_raw AS token_sold_amount_raw
     , dexs.amount_usd
     , dexs.token_bought_address
     , dexs.token_sold_address
     , dexs.taker
     , dexs.maker
     , dexs.project_contract_address
     , dexs.tx_hash
     , dexs.evt_index
FROM dexs