{{ config(
    schema = 'sushi_arbitrum',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
) }}

WITH token_swaps AS (
    SELECT
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) WITH time zone) AS block_time,
        evt_tx_from AS maker,
        evt_tx_to AS taker,
        amountIn AS token_sold_amount_raw,
        amountOut AS token_bought_amount_raw,
        tokenIn AS token_sold_address,
        tokenOut AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM 
        {{ source('sushiswap_v2_arbitrum', 'routeprocessor3_2_evt_route') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'arbitrum' AS blockchain,
    'sushi' AS project,
    '1' AS version,
    CAST(date_trunc('month', token_swaps.block_time) AS date) AS block_month,
    CAST(date_trunc('day', token_swaps.block_time) AS date) AS block_date,
    token_swaps.block_time,
    token_swaps.block_number,
    token_swaps.token_sold_amount_raw,
    token_swaps.token_bought_amount_raw,
    token_swaps.token_sold_address,
    token_swaps.token_bought_address,
    token_swaps.maker,
    token_swaps.taker,
    token_swaps.project_contract_address,
    token_swaps.tx_hash,
    token_swaps.evt_index
FROM 
    token_swaps