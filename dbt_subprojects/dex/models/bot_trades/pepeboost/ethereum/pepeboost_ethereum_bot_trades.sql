{{ config(
    alias = 'bot_trades',
    schema = 'pepeboost_ethereum',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'tx_hash', 'evt_index']
   )
}}

{% set project_name = 'Pepeboost' %}
{% set project_start_date = '2023-08-28' %}
{% set blockchain = 'ethereum' %}
{% set bot_deployer_1 = '0xeaBEe4C63bD085Fd906d6be5d387B5EEDfF83919' %}
{% set weth = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set fee_token_symbol = 'ETH' %}

WITH
  botContracts AS (
    SELECT
      address
    FROM
      {{ source('ethereum','creation_traces') }}
    WHERE
        "from" = {{bot_deployer_1}}
      AND block_time >= TIMESTAMP '{{project_start_date}}'
  ),
  botTrades AS (
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
      JOIN botContracts ON trades.tx_to = botContracts.address
    WHERE
      trades.blockchain = '{{blockchain}}'
      {% if is_incremental() %}
      AND {{ incremental_predicate('trades.block_time') }}
      {% else %}
      AND trades.block_time >= TIMESTAMP '{{project_start_date}}'
      {% endif %}
  ),
  highestEventIndexForEachTrade AS (
    SELECT
      tx_hash,
      MAX(evt_index) AS highestEventIndex
    FROM
      botTrades
    GROUP BY
      tx_hash
  ),
  botETHDeposits AS (
    SELECT
      tx_hash,
      block_number,
      CAST(value AS DECIMAL (38, 0)) AS deltaGwei,
      CAST(value AS DECIMAL (38, 0)) AS depositGwei
    FROM
      {{ source('ethereum','traces') }}
      JOIN botContracts ON to = botContracts.address
    WHERE
      {% if is_incremental() %}
      {{ incremental_predicate('block_time') }}
      {% else %}
      block_time >= TIMESTAMP '{{project_start_date}}'
      {% endif %}
      AND value > 0
  ),
  botETHWithdrawals AS (
    SELECT
      tx_hash,
      block_number,
      CAST(value AS DECIMAL (38, 0)) * -1 AS deltaGwei,
      0 AS depositGwei,
      block_hash,
      to
    FROM
      {{ source('ethereum','traces') }}
      JOIN botContracts ON "from" = botContracts.address
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
        deltaGwei,
        depositGwei
      FROM
        botETHDeposits
    )
    UNION ALL
    /* Withdrawals */
    (
      SELECT
        tx_hash,
        block_number,
        deltaGwei,
        depositGwei
      FROM
        botETHWithdrawals
    )
  ),
  botEthDeltas AS (
    SELECT
      tx_hash,
      block_number,
      SUM(deltaGwei) AS feeGwei,
      SUM(depositGwei) AS depositGwei
    FROM
      botEthTransfers
    GROUP BY
      tx_hash,
      block_number
  )
SELECT
  block_time,
  date_trunc('day', botTrades.block_time) as block_date,
  cast(date_trunc('month', botTrades.block_time) as date) as block_month,
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
    CAST(feeGwei AS DOUBLE) / CAST(depositGwei AS DOUBLE),
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
  botTrades.tx_hash,
  evt_index,
  IF(evt_index = highestEventIndex, true, false) AS is_last_trade_in_transaction
FROM
  botTrades
  JOIN highestEventIndexForEachTrade ON botTrades.tx_hash = highestEventIndexForEachTrade.tx_hash
  LEFT JOIN botETHDeltas ON botTrades.tx_hash = botETHDeltas.tx_hash
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