{{ config(
    alias = 'bot_trades',
    schema = 'flokibot_ethereum',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'tx_hash', 'evt_index']
   )
}}

{% set project_name = 'Floki Trading Bot' %}
{% set project_start_date = '2024-02-01' %}
{% set blockchain = 'ethereum' %}
{% set bot_deployer_1 = '0xdeb9E55E0F20bC59029271372ECea50E67182A3A' %}
{% set bot_deployer_2 = '0xcE6a13955EC32B6B1b7EBe089302b536Ad40aeC3' %}
{% set weth = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set fee_token_symbol = 'ETH' %}

WITH
  bot_contracts AS (
    SELECT
      address
    FROM
      {{ source('ethereum','creation_traces') }}
    WHERE
      ("from" = {{bot_deployer_1}} OR "from" = {{bot_deployer_2}})
      AND block_time >= TIMESTAMP '{{project_start_date}}'
  ),
  bot_trades AS (
    SELECT
      trades.block_time,
      amount_usd,
      IF(
        token_sold_address = {{weth}},
        'Buy',
        'Sell'
      ) AS type,
      token_bought_amount,
      token_bought_symbol,
      token_bought_address,
      token_sold_amount,
      token_sold_symbol,
      token_sold_address,
      project,
      version,
      token_pair,
      project_contract_address,
      tx_from AS user,
      tx_to AS bot,
      trades.tx_hash,
      evt_index
    FROM
      {{ source('dex', 'trades') }} as trades
      JOIN bot_contracts ON trades.tx_to = bot_contracts.address
    WHERE
      trades.blockchain = '{{blockchain}}'
      {% if is_incremental() %}
      AND {{ incremental_predicate('trades.block_time') }}
      {% else %}
      AND trades.block_time >= TIMESTAMP '{{project_start_date}}'
      {% endif %}
  ),
  highest_event_index_for_each_trade AS (
    SELECT
      tx_hash,
      MAX(evt_index) AS highest_event_index
    FROM
      bot_trades
    GROUP BY
      tx_hash
  ),
  bot_eth_deposits AS (
    SELECT
      tx_hash,
      block_number,
      CAST(value AS DECIMAL (38, 0)) AS delta_gwei,
      CAST(value AS DECIMAL (38, 0)) AS deposit_gwei
    FROM
      {{ source('ethereum','traces') }}
      JOIN bot_contracts ON to = bot_contracts.address
    WHERE
      {% if is_incremental() %}
      {{ incremental_predicate('block_time') }}
      {% else %}
      block_time >= TIMESTAMP '{{project_start_date}}'
      {% endif %}
      AND value > 0
  ),
  bot_eth_withdrawals AS (
    SELECT
      tx_hash,
      block_number,
      CAST(value AS DECIMAL (38, 0)) * -1 AS delta_gwei,
      0 AS deposit_gwei,
      block_hash,
      to
    FROM
      {{ source('ethereum','traces') }}
      JOIN bot_contracts ON "from" = bot_contracts.address
    WHERE
      {% if is_incremental() %}
      {{ incremental_predicate('block_time') }}
      {% else %}
      block_time >= TIMESTAMP '{{project_start_date}}'
      {% endif %}
      AND value > 0
  ),
  botEthTransfers AS (
    /* Deposits */
    (
      SELECT
        tx_hash,
        block_number,
        delta_gwei,
        deposit_gwei
      FROM
        bot_eth_deposits
    )
    UNION ALL
    /* Withdrawals */
    (
      SELECT
        tx_hash,
        block_number,
        delta_gwei,
        deposit_gwei
      FROM
        bot_eth_withdrawals
    )
  ),
  bot_eth_deltas AS (
    SELECT
      tx_hash,
      block_number,
      SUM(delta_gwei) AS fee_gwei,
      SUM(deposit_gwei) AS deposit_gwei
    FROM
      botEthTransfers
    GROUP BY
      tx_hash,
      block_number
  )
SELECT
  block_time,
  date_trunc('day', bot_trades.block_time) as block_date,
  date_trunc('month', bot_trades.block_time) as block_month,
  '{{project_name}}' as bot,
  block_number,
  '{{blockchain}}' AS blockchain,
  -- Trade
  amount_usd,
  type,
  token_bought_amount,
  token_bought_symbol,
  token_bought_address,
  token_sold_amount,
  token_sold_symbol,
  token_sold_address,
  -- Fees
  ROUND(
    CAST(fee_gwei AS DOUBLE) / CAST(deposit_gwei AS DOUBLE),
    /* Round feePercentage to 0.01% steps */
    4
  ) AS fee_percentage_fraction,
  (feeGwei / 1e18) * price AS fee_usd,
  feeGwei / 1e18 fee_token_amount,
  '{{fee_token_symbol}}' AS fee_token_symbol,
  {{weth}} AS fee_token_address,
  -- Dex
  project,
  version,
  token_pair,
  project_contract_address,
  -- User
  user AS user,
  bot_trades.tx_hash,
  evt_index,
  IF(evt_index = highest_event_index, true, false) AS is_last_trade_in_transaction
FROM
  bot_trades
  JOIN highest_event_index_for_each_trade ON bot_trades.tx_hash = highest_event_index_for_each_trade.tx_hash
  LEFT JOIN bot_eth_deltas ON bot_trades.tx_hash = bot_eth_deltas.tx_hash
  LEFT JOIN {{ source('prices', 'usd') }} ON (
    blockchain = '{{blockchain}}'
    AND contract_address = {{weth}}
    AND minute = DATE_TRUNC('minute', block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('minute') }}
    {% endif %}  
  )
ORDER BY
  block_time DESC,
  evt_index DESC