{{ config(
    schema = 'yieldbasis_arbitrum',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'tx_hash', 'evt_index']
) }}

/* Step 3: Final join and output */
/* Step 1: TokenExchange Events */
WITH token_exchange AS (
  SELECT
    evt_block_number AS block_number,
    TRY_CAST(evt_block_time AS TIMESTAMP(3) WITH TIME ZONE) AS block_time,
    buyer,
    evt_tx_from,
    evt_tx_to,
    tokens_sold AS token_sold_amount_raw,
    tokens_bought AS token_bought_amount_raw,
    sold_id,
    bought_id,
    contract_address AS pool_contract_address,
    evt_tx_hash AS tx_hash,
    evt_index
  FROM "delta_prod"."yieldbasis_arbitrum"."pool_test_evt_tokenexchange"
), coin_map /* Step 2: coinsfunction index-to-address mapping */ AS (
  SELECT
    contract_address AS pool_address,
    arg0 AS token_index,
    output_0 AS token_address
  FROM "delta_prod"."yieldbasis_arbitrum"."pool_test_call_coins"
  WHERE
    call_success = TRUE
  GROUP BY
    1,
    2,
    3
)
SELECT
  'arbitrum' AS blockchain,
  'yieldbasis' AS project,
  '1' AS version,
  TRY_CAST(DATE_TRUNC('month', te.block_time) AS DATE) AS block_month,
  TRY_CAST(DATE_TRUNC('day', te.block_time) AS DATE) AS block_date,
  te.block_time,
  te.block_number,
  te.token_sold_amount_raw,
  te.token_bought_amount_raw,
  sold_token.token_address AS token_sold_address,
  bought_token.token_address AS token_bought_address,
  te.buyer AS taker,
  te.evt_tx_from AS maker,
  te.evt_tx_to,
  te.pool_contract_address AS project_contract_address,
  te.tx_hash,
  te.evt_index
FROM token_exchange AS te
LEFT JOIN coin_map AS sold_token
  ON te.pool_contract_address = sold_token.pool_address
  AND te.sold_id = sold_token.token_index
LEFT JOIN coin_map AS bought_token
  ON te.pool_contract_address = bought_token.pool_address
  AND te.bought_id = bought_token.token_index