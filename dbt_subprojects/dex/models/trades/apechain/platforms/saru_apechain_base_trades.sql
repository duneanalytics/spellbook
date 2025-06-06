{{ config(
    schema = 'saru_apechain',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
) }}

WITH swap_events AS (
    SELECT
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) with time zone) AS block_time,
        evt_tx_from AS maker,
        contract_address AS taker,
        COALESCE(amount0In, 0) + COALESCE(amount1In, 0) AS token_sold_amount_raw,
        COALESCE(amount0Out, 0) + COALESCE(amount1Out, 0) AS token_bought_amount_raw,
        CAST(sender AS varbinary) AS token_sold_address,
        CAST(to AS varbinary) AS token_bought_address,
        CAST(contract_address AS varbinary) AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM
        {{ source('saru_apechain', 'sarupair_evt_swap') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'apechain' AS blockchain,
    'saru' AS project,
    '1' AS version,
    CAST(date_trunc('month', swap_events.block_time) AS date) AS block_month,
    CAST(date_trunc('day', swap_events.block_time) AS date) AS block_date,
    swap_events.block_time,
    swap_events.block_number,
    swap_events.token_sold_amount_raw,
    swap_events.token_bought_amount_raw,
    swap_events.token_sold_address,
    swap_events.token_bought_address,
    swap_events.maker,
    swap_events.taker,
    swap_events.project_contract_address,
    swap_events.tx_hash,
    swap_events.evt_index
FROM
    swap_events
