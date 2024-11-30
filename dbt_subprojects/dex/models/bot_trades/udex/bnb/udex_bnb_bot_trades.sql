{{ config(
    alias = 'bot_trades',
    schema = 'udex_bnb',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'tx_hash', 'evt_index']
   )
}}

{% set project_name = 'Udex' %}
{% set project_start_date = '2023-12-24' %}
{% set blockchain = 'bnb' %}
{% set vault = '0x1df00191a32184675baA3fc0416A57009C386ed9' %}
{% set wbnb = '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c' %}
{% set fee_token_symbol = 'BNB' %}

  WITH swaps AS (
    SELECT DISTINCT
      'Market' AS order_type,
      evt_block_time,
      evt_block_number,
      evt_tx_hash,
      contract_address AS router
    FROM
      {{ source('udex_bnb', 'SpotSwapRouter_evt_Swap') }}
  ),
  limit_orders_filled AS (
    SELECT DISTINCT
      'Limit' AS order_type,
      evt_block_time,
      evt_block_number,
      evt_tx_hash,
      contract_address AS router
    FROM
      {{ source('udex_bnb', 'SpotLimitOrderRouter_evt_SpotOrderFilled') }}
  ),
  trades_tx_hashes AS (
    SELECT
      *
    FROM
      swaps
    UNION ALL
    SELECT
      *
    FROM
      limit_orders_filled
  ),
  bot_trades AS (
    SELECT
      block_time,
      trades_tx_hashes.evt_block_number AS block_number,
      amount_usd,
      order_type,
      IF(
        token_sold_address = {{wbnb}}, -- WBNB
        'Buy',
        'Sell'
      ) AS type,
      token_bought_amount,
      token_bought_symbol,
      token_bought_address,
      token_sold_amount,
      token_sold_symbol,
      token_sold_address,
      (fee_payments.value / POWER(10, decimals)) * price AS fee_usd,
      (fee_payments.value / POWER(10, decimals)) AS fee_token_amount,
      '{{fee_token_symbol}}' AS fee_token_symbol,
      {{ wbnb }} AS fee_token_address,
      project,
      version,
      token_pair,
      CAST(project_contract_address AS VARCHAR) AS project_contract_address,
      CAST(tx_from AS VARCHAR) AS user,
      router AS bot,
      CAST(tx_hash AS VARCHAR) AS tx_hash,
      trades.evt_index
    FROM
      trades_tx_hashes
      JOIN {{ source('dex', 'trades') }} as trades ON (
        trades_tx_hashes.evt_tx_hash = tx_hash
        AND trades_tx_hashes.evt_block_time = block_time
      )
      LEFT JOIN {{ source('erc20_bnb','evt_transfer') }} as fee_payments ON (
        fee_payments.evt_tx_hash = tx_hash
        AND fee_payments.evt_block_time = block_time
        AND fee_payments.contract_address = {{ wbnb }}
        AND "from" = router
        AND to = {{vault}}
      )
      LEFT JOIN {{ source('prices', 'usd') }} AS fee_token_prices ON (
        fee_token_prices.blockchain = 'bnb'
        AND fee_token_prices.contract_address = {{ wbnb }}
        AND date_trunc('minute', block_time) = minute
      )
    WHERE
    trades.blockchain = 'bnb'
  ),
  highest_event_index_for_each_trade AS (
    SELECT
      tx_hash,
      MAX(evt_index) AS highest_event_index
    FROM
      bot_trades
    GROUP BY
      tx_hash
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
    CAST(fee_usd AS DOUBLE) / CAST(amount_usd AS DOUBLE),
    4 -- Round feePercentage to 0.01% steps
  ) AS fee_percentage_fraction,
  fee_usd,
  fee_token_amount,
  fee_token_symbol,
  fee_token_address,
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
ORDER BY
  block_time DESC,
  evt_index DESC