{{ config(
    alias = 'bot_trades',
    schema = 'banana_gun_sonic',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'tx_hash', 'evt_index']
   )
}}

{% set project_name = 'Banana Gun' %}
{% set project_start_date = '2024-12-10' %}
{% set blockchain = 'sonic' %}
{% set bot_deployer_1 = '0x37aAb97476bA8dC785476611006fD5dDA4eed66B' %}
{% set wrapped_sonic = '0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38' %}
{% set fee_token_symbol = 'S' %}

WITH
  botContracts AS (
    SELECT
      address
    FROM
      {{ source('sonic','creation_traces') }}
    WHERE
      (
        "from" = {{bot_deployer_1}}
      )
      AND block_time >= TIMESTAMP '{{project_start_date}}'
  ),
  botTrades AS (
    SELECT
      trades.block_time,
      amount_usd,
      IF(
        token_sold_address = {{wrapped_sonic}},
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
  botSONICDeposits AS (
    SELECT
      tx_hash,
      block_number,
      CAST(value AS DECIMAL (38, 0)) AS deltaGwei,
      CAST(value AS DECIMAL (38, 0)) AS depositGwei
    FROM
      {{ source('sonic','traces') }}
      JOIN botContracts ON to = botContracts.address
    WHERE
      {% if is_incremental() %}
      {{ incremental_predicate('block_time') }}
      {% else %}
      block_time >= TIMESTAMP '{{project_start_date}}'
      {% endif %}
      AND value > CAST(0 AS UINT256)
  ),
  botSONICWithdrawals AS (
    SELECT
      tx_hash,
      block_number,
      CAST(value AS DECIMAL (38, 0)) * -1 AS deltaGwei,
      0 AS depositGwei,
      block_hash,
      to
    FROM
      {{ source('sonic','traces') }}
      JOIN botContracts ON "from" = botContracts.address
    WHERE
      {% if is_incremental() %}
      {{ incremental_predicate('block_time') }}
      {% else %}
      block_time >= TIMESTAMP '{{project_start_date}}'
      {% endif %}
      AND value > CAST(0 AS UINT256)
  ),
  botSONICTransfers AS (
    /* Deposits */
    (
      SELECT
        tx_hash,
        block_number,
        deltaGwei,
        depositGwei
      FROM
        botSONICDeposits
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
        botSONICWithdrawals
    )
  ),
  botSONICDeltas AS (
    SELECT
      tx_hash,
      block_number,
      SUM(deltaGwei) AS feeGwei,
      SUM(depositGwei) AS depositGwei
    FROM
      botSONICTransfers
    GROUP BY
      tx_hash,
      block_number
  )
SELECT
  block_time,
  date_trunc('day', botTrades.block_time) as block_date,
  date_trunc('month', botTrades.block_time) as block_month,
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
  {{wrapped_sonic}} AS fee_token_address,
  -- Dex
  project,
  version,
  token_pair,
  project_contract_address,
  -- User
  user,
  botTrades.tx_hash,
  evt_index,
  IF(evt_index = highestEventIndex, true, false) AS is_last_trade_in_transaction
FROM
  botTrades
  JOIN highestEventIndexForEachTrade ON botTrades.tx_hash = highestEventIndexForEachTrade.tx_hash
  LEFT JOIN botSONICDeltas ON botTrades.tx_hash = botSONICDeltas.tx_hash
  LEFT JOIN {{ source('prices', 'usd') }} ON (
    blockchain = '{{blockchain}}'
    AND contract_address = {{wrapped_sonic}}
    AND minute = DATE_TRUNC('minute', block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('minute') }}
    {% endif %}  
  )
ORDER BY
  block_time DESC,
  evt_index DESC