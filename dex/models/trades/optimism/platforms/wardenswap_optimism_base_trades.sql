{{
    config(
        schema = 'wardenswap_optimism',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS (
    SELECT
        evt_block_number AS block_number,
        evt_block_time AS block_time,
        trader AS taker,
        receiver AS maker,
        destAmount AS token_bought_amount_raw,
        srcAmount AS token_sold_amount_raw,
        destAsset AS token_bought_address,
        srcAsset AS token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('wardenswap_optimism', 'WardenRouterV2_L2_evt_Trade') }} t
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    'optimism' AS blockchain,
    'wardenswap' AS project,
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
