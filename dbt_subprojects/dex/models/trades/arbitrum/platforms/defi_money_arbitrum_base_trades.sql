{% if source('defi_money_arbitrum', 'arb_amm_evt_tokenexchange') is not none %}

{{ config(
  schema = 'defi_money_arbitrum',
  alias = 'base_trades',
  partition_by = ['block_month'],
  materialized = 'incremental',
  file_format = 'delta',
  incremental_strategy = 'merge',
  unique_key = ['tx_hash', 'evt_index']
) }}

-- Step 1: Combine all `coins()` calls to map pool + index -> token_address
WITH coins_calls AS (

  SELECT contract_address AS pool_address, i, output_0 AS token_address, call_block_number, call_block_time
  FROM {{ source('defi_money_arbitrum', 'arb_amm_call_coins') }}
  WHERE call_success = TRUE

  UNION ALL
  SELECT contract_address, i, output_0, call_block_number, call_block_time
  FROM {{ source('defi_money_arbitrum', 'wbtc_amm_call_coins') }}
  WHERE call_success = TRUE

  UNION ALL
  SELECT contract_address, i, output_0, call_block_number, call_block_time
  FROM {{ source('defi_money_arbitrum', 'weth_amm_call_coins') }}
  WHERE call_success = TRUE
),

latest_coins AS (
  SELECT
    pool_address,
    i,
    token_address,
    ROW_NUMBER() OVER (PARTITION BY pool_address, i ORDER BY call_block_number DESC, call_block_time DESC) AS rn
  FROM coins_calls
),

coins_mapping AS (
  SELECT pool_address,
         MAX(CASE WHEN i = 0 THEN token_address END) AS token0_address,
         MAX(CASE WHEN i = 1 THEN token_address END) AS token1_address
  FROM latest_coins
  WHERE rn = 1
  GROUP BY pool_address
),

-- Step 2: Combine all TokenExchange swap events
swap_events AS (

  SELECT * FROM {{ source('defi_money_arbitrum', 'arb_amm_evt_tokenexchange') }}
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  UNION ALL
  SELECT * FROM {{ source('defi_money_arbitrum', 'wbtc_amm_evt_tokenexchange') }}
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}

  UNION ALL
  SELECT * FROM {{ source('defi_money_arbitrum', 'weth_amm_evt_tokenexchange') }}
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}
)

-- Step 3: Final output
SELECT
  'arbitrum' AS blockchain,
  'defi_money' AS project,
  '1' AS version,
  TRY_CAST(DATE_TRUNC('month', evt_block_time) AS DATE) AS block_month,
  TRY_CAST(DATE_TRUNC('day', evt_block_time) AS DATE) AS block_date,
  evt_block_time AS block_time,
  evt_block_number AS block_number,
  CASE WHEN sold_id = 0 THEN tokens_sold ELSE NULL END AS token0_sold_amount_raw,
  CASE WHEN sold_id = 1 THEN tokens_sold ELSE NULL END AS token1_sold_amount_raw,
  CASE WHEN bought_id = 0 THEN tokens_bought ELSE NULL END AS token0_bought_amount_raw,
  CASE WHEN bought_id = 1 THEN tokens_bought ELSE NULL END AS token1_bought_amount_raw,
  CASE WHEN sold_id = 0 THEN cm.token0_address ELSE cm.token1_address END AS token_sold_address,
  CASE WHEN bought_id = 0 THEN cm.token0_address ELSE cm.token1_address END AS token_bought_address,
  buyer AS maker,
  evt_tx_to AS taker,
  contract_address AS project_contract_address,
  evt_tx_hash AS tx_hash,
  evt_index
FROM swap_events
JOIN coins_mapping cm ON swap_events.contract_address = cm.pool_address

{% else %}

-- Fallback when schema is missing
SELECT
  'arbitrum' AS blockchain,
  'defi_money' AS project,
  '1' AS version,
  CURRENT_DATE AS block_month,
  CURRENT_DATE AS block_date,
  CURRENT_TIMESTAMP AS block_time,
  NULL AS block_number,
  NULL AS token0_sold_amount_raw,
  NULL AS token1_sold_amount_raw,
  NULL AS token0_bought_amount_raw,
  NULL AS token1_bought_amount_raw,
  NULL AS token_sold_address,
  NULL AS token_bought_address,
  NULL AS maker,
  NULL AS taker,
  NULL AS project_contract_address,
  NULL AS tx_hash,
  NULL AS evt_index
LIMIT 0

{% endif %}
