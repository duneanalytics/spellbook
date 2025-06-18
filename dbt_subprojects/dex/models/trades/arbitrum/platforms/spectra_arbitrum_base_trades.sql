{{ config(
    schema = 'spectra_arbitrum',
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
        buyer AS maker,
        evt_tx_to AS taker,
        tokens_sold AS token_sold_amount_raw,
        tokens_bought AS token_bought_amount_raw,
        sold_id,
        bought_id,
        contract_address AS contract_address,  -- Rename for clarity
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index
    FROM
        {{ source('spectra_multichain', 'vyper_contract_evt_tokenexchange') }}
    WHERE chain = 'arbitrum'
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

-- Get token addresses from coins function calls
coin_mapping AS (
    SELECT
        contract_address AS pool_address,
        arg0 AS token_index,
        output_0 AS token_address
    FROM
        {{ source('spectra_multichain', 'vyper_contract_call_coins') }}
    WHERE
        call_success = TRUE
    {% if is_incremental() %}
    AND {{ incremental_predicate('call_block_time') }}
    {% endif %}
    GROUP BY 1, 2, 3  -- Remove duplicates
)

SELECT
    'arbitrum' AS blockchain,
    'spectra' AS project,
    '1' AS version,
    CAST(date_trunc('month', ts.block_time) AS date) AS block_month,
    CAST(date_trunc('day', ts.block_time) AS date) AS block_date,
    ts.block_time,
    ts.block_number,
    ts.token_sold_amount_raw,
    ts.token_bought_amount_raw,
    sold_coins.token_address AS token_sold_address,  -- Resolved from coins
    bought_coins.token_address AS token_bought_address,  -- Resolved from coins
    ts.maker,
    ts.taker,
    ts.contract_address,
    ts.tx_hash,
    ts.evt_index
FROM
    token_swaps ts
LEFT JOIN
    coin_mapping sold_coins
    ON ts.contract_address = sold_coins.pool_address
    AND ts.sold_id = sold_coins.token_index
LEFT JOIN
    coin_mapping bought_coins
    ON ts.contract_address = bought_coins.pool_address
    AND ts.bought_id = bought_coins.token_index
