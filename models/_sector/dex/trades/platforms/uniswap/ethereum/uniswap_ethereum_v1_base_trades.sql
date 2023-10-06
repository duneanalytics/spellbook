{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v1_ethereum',
    alias = 'base_trades',
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
         ,t.buyer              AS taker
         ,CAST(NULL as VARBINARY) as maker
         ,t.tokens_bought AS token_bought_amount_raw
         ,t.eth_sold AS token_sold_amount_raw
         ,f.token AS token_bought_address
         ,{{weth_address}} AS token_sold_address --Using WETH for easier joining with USD price table
         ,t.contract_address AS project_contract_address
         ,t.evt_tx_hash AS tx_hash
         ,t.evt_index
    FROM {{ source('uniswap_ethereum', 'Exchange_evt_TokenPurchase') }} t
    INNER JOIN {{ source('uniswap_ethereum', 'Factory_evt_NewExchange') }} f
        ON f.exchange = t.contract_address
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}

    UNION ALL

    -- Uniswap v1 EthPurchase
    SELECT t.evt_block_time     AS block_time
         ,t.buyer              AS taker
         ,CAST(NULL as VARBINARY) as maker
         ,t.eth_bought AS token_bought_amount_raw
         ,t.tokens_sold AS token_sold_amount_raw
         ,{{weth_address}} AS token_bought_address --Using WETH for easier joining with USD price table
         ,f.token AS token_sold_address
         ,t.contract_address AS project_contract_address
         ,t.evt_tx_hash AS tx_hash
         ,t.evt_index
    FROM {{ source('uniswap_ethereum', 'Exchange_evt_EthPurchase') }} t
    INNER JOIN {{ source('uniswap_ethereum', 'Factory_evt_NewExchange') }} f
        ON f.exchange = t.contract_address
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}
)

SELECT 
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    ,CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , dexs.block_time
    , dexs.token_bought_amount_raw  AS token_bought_amount_raw
    , dexs.token_sold_amount_raw AS token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM dexs