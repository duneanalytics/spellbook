{{ config(
    schema = 'curvefi_base',
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
        CAST(evt_block_time AS timestamp(3) with time zone) AS block_time,
        evt_tx_from AS maker,
        evt_tx_to AS taker,
        tokens_sold AS token_sold_amount_raw,
        tokens_bought AS token_bought_amount_raw,
        CAST(sold_id AS varbinary) AS token_sold_address,
        CAST(bought_id AS varbinary) AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM
        {{ source('curvefi_base', 'StableSwap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'base' AS blockchain,
    'curvefi' AS project,
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
