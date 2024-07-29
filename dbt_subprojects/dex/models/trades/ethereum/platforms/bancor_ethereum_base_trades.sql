{{
    config(
        schema = 'bancor_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}

WITH conversions AS (
    {% for n in range(6,11) %}
        SELECT
            t.evt_block_number,
            t.evt_block_time,
            t._trader,
            t._toAmount,
            t._fromAmount,
            t._toToken,
            t._fromToken,
            t.contract_address,
            t.evt_tx_hash,
            t.evt_index
        FROM {{ source('bancornetwork_ethereum', 'BancorNetwork_v' ~ n ~ '_evt_Conversion' )}} t
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),

dexs AS (
    SELECT
        '1' AS version,
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t._trader AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        t._toAmount AS token_bought_amount_raw,
        t._fromAmount AS token_sold_amount_raw,
        CAST(NULL as double) AS amount_usd,
        CASE
            WHEN t._toToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN {{ weth_address }}
            ELSE t._toToken
        END AS token_bought_address,
        CASE
            WHEN t._fromToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN {{ weth_address }}
            ELSE t._fromToken
        END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM conversions t

    UNION ALL

    SELECT
        '3' AS version,
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.trader AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        t.targetAmount AS token_bought_amount_raw,
        t.sourceAmount AS token_sold_amount_raw,
        CAST(NULL as double) AS amount_usd,
        CASE
            WHEN t.targetToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN {{ weth_address }}
            ELSE t.targetToken
        END AS token_bought_address,
        CASE
            WHEN t.sourceToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN {{ weth_address }}
            ELSE t.sourceToken
        END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM {{ source('bancor3_ethereum', 'BancorNetwork_evt_TokensTraded') }} t
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'ethereum' AS blockchain,
    'bancor' AS project,
    dexs.version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM dexs
