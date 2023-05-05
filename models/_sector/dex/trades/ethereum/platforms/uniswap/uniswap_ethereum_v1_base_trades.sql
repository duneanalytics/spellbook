{{ config(
    schema = 'uniswap_ethereum',
    alias = 'v1_base_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2018-11-01' %}
{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

WITH dexs AS
(
    -- Uniswap v1 TokenPurchase
    SELECT t.evt_block_time     AS block_time
         , t.buyer              AS taker
         , ''                   AS maker
         , t.tokens_bought      AS token_bought_amount_raw
         , t.eth_sold           AS token_sold_amount_raw
         , CAST(NULL AS DOUBLE) AS amount_usd
         , f.token              AS token_bought_address
         , '{{weth_address}}'   AS token_sold_address --Using WETH for easier joining with USD price table
         , t.contract_address   AS project_contract_address
         , t.evt_tx_hash        AS tx_hash
         , ''                   AS trace_address
         , t.evt_index
    FROM {{ source('uniswap_ethereum', 'Exchange_evt_TokenPurchase') }} t
    INNER JOIN {{ source('uniswap_ethereum', 'Factory_evt_NewExchange') }} f
        ON f.exchange = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION ALL

    -- Uniswap v1 EthPurchase
    SELECT t.evt_block_time     AS block_time
         , t.buyer              AS taker
         , ''                   AS maker
         , t.eth_bought         AS token_bought_amount_raw
         , t.tokens_sold        AS token_sold_amount_raw
         , CAST(NULL AS DOUBLE) AS amount_usd
         , '{{weth_address}}'   AS token_bought_address --Using WETH for easier joining with USD price tabl
         , f.token              AS token_sold_address
         , t.contract_address   AS project_contract_address
         , t.evt_tx_hash        AS tx_hash
         , ''                   AS trace_address
         , t.evt_index
    FROM {{ source('uniswap_ethereum', 'Exchange_evt_EthPurchase') }} t
    INNER JOIN {{ source('uniswap_ethereum', 'Factory_evt_NewExchange') }} f
        ON f.exchange = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
     , dexs.block_time
     , CAST(dexs.token_bought_amount_raw AS DECIMAL(38, 0)) AS token_bought_amount_raw
     , CAST(dexs.token_sold_amount_raw AS DECIMAL(38, 0))   AS token_sold_amount_raw
     , dexs.amount_usd
     , dexs.token_bought_address
     , dexs.token_sold_address
     , dexs.taker
     , dexs.maker
     , dexs.project_contract_address
     , dexs.tx_hash
     , dexs.trace_address
     , dexs.evt_index
FROM dexs
;