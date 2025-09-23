{{
    config(
        schema = 'machinex_v3_peaq',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set machinex_start_date = "2025-05-25" %}

WITH dexs AS (
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.recipient AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw,
        CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw,
        CASE WHEN amount0 < INT256 '0' THEN f.tokena ELSE f.tokenb END AS token_bought_address,
        CASE WHEN amount0 < INT256 '0' THEN f.tokenb ELSE f.tokena END AS token_sold_address,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM
        {{ source('machinex_peaq', 'machinexv3pool_evt_swap') }} t
    INNER JOIN
        {{ source('machinex_peaq', 'machinexv3factory_call_createpool') }} f
        ON f.output_pool = t.contract_address
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {% else %}
    WHERE
        t.evt_block_time >= TIMESTAMP '{{machinex_start_date}}'
    {% endif %}
)

SELECT
    'peaq' AS blockchain,
    'machinex' AS project,
    '3' AS version,
    CAST(DATE_TRUNC('month', dexs.block_time) AS DATE) AS block_month,
    CAST(DATE_TRUNC('day', dexs.block_time) AS DATE) AS block_date,
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