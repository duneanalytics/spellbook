{{
    config(
        schema = 'izumi_finance_taiko',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set izumi_finance_start_date = "2024-05-27" %}  

WITH swaps AS (
    SELECT
        contract_address,
        evt_tx_hash AS tx_hash,
        evt_index,
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        evt_block_date AS block_date,
        evt_tx_to AS taker,
        sellXEarnY,
        amountX,
        amountY
    FROM {{ source('izumi_finance_taiko', 'iziswappool_evt_swap') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% else %}
    WHERE evt_block_time >= TIMESTAMP '{{izumi_finance_start_date}}'
    {% endif %}
),
pools AS (
    SELECT
        pool,
        tokenX,
        tokenY,
        fee
    FROM {{ source('izumi_finance_multichain', 'iziswapfactory_evt_newpool') }}
    WHERE chain = 'taiko'
)
SELECT
    'taiko' AS blockchain,
    'izumi_finance' AS project,
    '1' AS version,
    CAST(DATE_TRUNC('month', swaps.block_time) AS DATE) AS block_month,
    CAST(DATE_TRUNC('day', swaps.block_time) AS DATE) AS block_date,
    swaps.block_time,
    swaps.block_number,
    CASE WHEN swaps.sellXEarnY THEN pools.tokenY ELSE pools.tokenX END AS token_bought_address,
    CASE WHEN swaps.sellXEarnY THEN pools.tokenX ELSE pools.tokenY END AS token_sold_address,
    CASE WHEN swaps.sellXEarnY THEN swaps.amountY ELSE swaps.amountX END AS token_bought_amount_raw,
    CASE WHEN swaps.sellXEarnY THEN swaps.amountX ELSE swaps.amountY END AS token_sold_amount_raw,
    swaps.taker,
    CAST(NULL AS VARBINARY) AS maker,
    swaps.contract_address AS project_contract_address,
    swaps.tx_hash,
    swaps.evt_index,
    pools.fee AS pool_fee
FROM swaps
INNER JOIN pools
    ON pools.pool = swaps.contract_address