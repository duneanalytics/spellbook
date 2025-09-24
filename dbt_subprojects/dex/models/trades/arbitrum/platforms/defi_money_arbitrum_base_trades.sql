{{ config(
    schema = 'defi_money_arbitrum',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
) }}

-- Step 1: Token Swap Events
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
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ source('defi_money_arbitrum', 'arb_amm_evt_tokenexchange') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),

-- Step 2: Coin Mapping (i => token address)
coin_mapping AS (
    SELECT
        contract_address AS pool_address,
        i AS token_index,
        output_0 AS token_address
    FROM {{ source('defi_money_arbitrum', 'arb_amm_call_coins') }}
    WHERE call_success = TRUE
    {% if is_incremental() %}
    AND {{ incremental_predicate('call_block_time') }}
    {% endif %}
    GROUP BY 1, 2, 3
)

-- Step 3: Final Output
SELECT
    'arbitrum' AS blockchain,
    'defi_money' AS project,
    '1' AS version,
    CAST(date_trunc('month', ts.block_time) AS date) AS block_month,
    CAST(date_trunc('day', ts.block_time) AS date) AS block_date,
    ts.block_time,
    ts.block_number,
    ts.token_sold_amount_raw,
    ts.token_bought_amount_raw,
    sold_coins.token_address AS token_sold_address,
    bought_coins.token_address AS token_bought_address,
    ts.maker,
    ts.taker,
    ts.project_contract_address,
    ts.tx_hash,
    ts.evt_index
FROM token_swaps ts
LEFT JOIN coin_mapping sold_coins
    ON ts.project_contract_address = sold_coins.pool_address
    AND ts.sold_id = sold_coins.token_index
LEFT JOIN coin_mapping bought_coins
    ON ts.project_contract_address = bought_coins.pool_address
    AND ts.bought_id = bought_coins.token_index