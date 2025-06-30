{{ config(
    schema = 'stratus_plume',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
) }}

WITH route_events AS (
    SELECT
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) WITH time zone) AS block_time,
        "from" AS maker,
        "to" AS taker,
        amountIn AS token_sold_amount_raw,
        amountOut AS token_bought_amount_raw,
        tokenIn AS token_sold_address,
        tokenOut AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM
        {{ source('stratus_plume', 'stratusrouter_evt_route') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'plume' AS blockchain,
    'stratus' AS project,
    '1' AS version,
    CAST(date_trunc('month', route_events.block_time) AS date) AS block_month,
    CAST(date_trunc('day', route_events.block_time) AS date) AS block_date,
    route_events.block_time,
    route_events.block_number,
    route_events.token_sold_amount_raw,
    route_events.token_bought_amount_raw,
    route_events.token_sold_address,
    route_events.token_bought_address,
    route_events.maker,
    route_events.taker,
    route_events.project_contract_address,
    route_events.tx_hash,
    route_events.evt_index
FROM
    route_events
