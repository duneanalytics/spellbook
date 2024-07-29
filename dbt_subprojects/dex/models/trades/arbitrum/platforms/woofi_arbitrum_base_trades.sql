{{
    config(
        schema = 'woofi_arbitrum',
        alias ='base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS
(
        SELECT
            t.evt_block_number AS block_number,
            t.evt_block_time AS block_time,
            t."from" AS taker,
            t.to AS maker,
            t.fromAmount AS token_sold_amount_raw,
            t.toAmount AS token_bought_amount_raw,
            t.fromToken AS token_sold_address,
            t.toToken AS token_bought_address,
            t.contract_address AS project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index
        FROM {{ source('woofi_swap_arbitrum', 'WooPPV2_1_evt_WooSwap') }} t
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
)

SELECT
    'arbitrum' AS blockchain,
    'woofi' AS project,
    '2' AS version,
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