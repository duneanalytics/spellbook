{{ config(
    schema = 'defiswap_ethereum',
    alias ='base_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'trace_address']
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
        ,CASE WHEN amount0Out = '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        ,CASE WHEN amount0In = '0' OR amount1Out = '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        ,CASE WHEN amount0Out = '0' THEN f.token1 ELSE f.token0 END AS token_bought_address
        ,CASE WHEN amount0In = '0' OR amount1Out = '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        ,t.contract_address AS project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,'' AS trace_address
        ,t.evt_index AS evt_index
    FROM {{ source('defiswap_ethereum', 'CroDefiSwapPair_evt_Swap') }} t
    INNER JOIN {{ source('crodefi_ethereum', 'CroDefiSwapFactory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
     , dexs.block_time
     , CAST(dexs.token_bought_amount_raw AS DECIMAL(38, 0)) AS token_bought_amount_raw
     , CAST(dexs.token_sold_amount_raw AS DECIMAL(38, 0))   AS token_sold_amount_raw
     , dexs.token_bought_address
     , dexs.token_sold_address
     , dexs.taker
     , dexs.maker
     , dexs.project_contract_address
     , dexs.tx_hash
     , dexs.trace_address
     , dexs.evt_index
FROM dexs