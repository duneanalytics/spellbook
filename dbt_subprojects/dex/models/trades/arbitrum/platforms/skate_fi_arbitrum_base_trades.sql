{{ config(
    schema = 'skate_fi_arbitrum',
    alias = 'vertex_vault_swaps',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_tx_hash', 'evt_index']
) }}

WITH swaps AS (
    SELECT
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) WITH time zone) AS block_time,
        CAST(evt_block_date AS date) AS block_date,
        evt_tx_from AS sender,
        evt_tx_to AS receiver,
        amountIn AS token_sold_amount_raw, 
        amountOut AS token_bought_amount_raw,
        tokenIn AS token_sold_address,
        tokenOut AS token_bought_address,
        CAST(contract_address AS varbinary) AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_tx_index AS tx_index,
        evt_index
    FROM
        {{ source('skate_fi_arbitrum', 'SkateVertexVault_evt_Swapped') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'arbitrum' AS blockchain,
    'skate_fi' AS project,
    '1' AS version,
    'vertex_vault' AS contract,
    CAST(date_trunc('month', swaps.block_time) AS date) AS block_month,
    swaps.block_date,
    swaps.block_time,
    swaps.block_number,
    swaps.token_sold_amount_raw,
    swaps.token_bought_amount_raw,
    swaps.token_sold_address,
    swaps.token_bought_address,
    swaps.sender AS taker,
    swaps.receiver AS maker,
    swaps.project_contract_address,
    swaps.tx_hash,
    swaps.tx_index,
    swaps.evt_index
FROM
    swaps
