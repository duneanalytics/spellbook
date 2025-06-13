{{ config(
    schema = 'bmx_bmx',
    alias = 'bmx_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
) }}

WITH router_swaps AS (
    SELECT
        CAST(evt_block_time AS timestamp(3) with time zone) AS block_time,
        evt_block_number AS block_number,
        CAST(date_trunc('day', evt_block_time) AS date) AS block_date,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index,
        evt_tx_from AS maker,
        evt_tx_to AS taker,
        amountIn AS token_sold_amount_raw,
        amountOut AS token_bought_amount_raw,
        tokenIn AS token_sold_address,
        tokenOut AS token_bought_address
    FROM {{ source('bmx_multichain', 'router_evt_swap') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

vault_swaps AS (
    SELECT
        CAST(evt_block_time AS timestamp(3) with time zone) AS block_time,
        evt_block_number AS block_number,
        CAST(date_trunc('day', evt_block_time) AS date) AS block_date,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index,
        evt_tx_from AS maker,
        evt_tx_to AS taker,
        amountIn AS token_sold_amount_raw,
        amountOutAfterFees AS token_bought_amount_raw,
        tokenIn AS token_sold_address,
        tokenOut AS token_bought_address
    FROM {{ source('bmx_multichain', 'vault_evt_swap') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

all_swaps AS (
    SELECT * FROM router_swaps
    UNION ALL
    SELECT * FROM vault_swaps
)

SELECT
    'bmx' AS blockchain,
    'bmx' AS project,
    '1' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    block_date,
    block_time,
    block_number,
    token_sold_amount_raw,
    token_bought_amount_raw,
    token_sold_address,
    token_bought_address,
    maker,
    taker,
    project_contract_address,
    tx_hash,
    evt_index
FROM all_swaps
