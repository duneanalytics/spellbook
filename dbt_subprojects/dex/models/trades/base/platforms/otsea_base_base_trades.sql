{{ config(
    schema = 'otsea_base',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
) }}

WITH token_swaps AS (
    SELECT
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) with time zone) AS block_time,
        evt_tx_from AS maker,
        evt_tx_to AS taker,
        swapped AS token_sold_amount_raw,
        received AS token_bought_amount_raw,
        CAST(token AS varbinary) AS token_sold_address,
        0x0000000000000000000000000000000000000000 AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM
        {{ source('otsea_base', 'OTSeaV2_evt_SwappedTokensForETH') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),
eth_swaps AS (
    SELECT
        evt_block_number AS block_number,
        CAST(evt_block_time AS timestamp(3) with time zone) AS block_time,
        evt_tx_from AS maker,
        evt_tx_to AS taker,
        swapped AS token_sold_amount_raw,
        received AS token_bought_amount_raw,
        0x0000000000000000000000000000000000000000 AS token_sold_address,
        CAST(token AS varbinary) AS token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM
        {{ source('otsea_base', 'OTSeaV2_evt_SwappedETHForTokens') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    'base' AS blockchain,
    'otsea' AS project,
    '1' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day', block_time) AS date) AS block_date,
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
FROM
    token_swaps
UNION ALL
SELECT
    'base' AS blockchain,
    'otsea' AS project,
    '1' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day', block_time) AS date) AS block_date,
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
FROM
    eth_swaps