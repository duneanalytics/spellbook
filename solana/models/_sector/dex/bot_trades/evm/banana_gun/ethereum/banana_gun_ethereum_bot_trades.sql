{{ config(
    alias = 'bot_trades',
    schema = 'banana_gun_solana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'tx_id', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']
   )
}}

{% set project_start_date = '2024-01-08' %}
{% set fee_receiver_1 = '8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2' %}
{% set fee_receiver_2 = 'Cj297UauzMX64FU9dKJZRUBWszJ7tEWpVheasq4CfATV' %}
{% set fee_receiver_3 = 'HKMh8nV3ysSofRi23LsfVGLGQKB415QAEfZT96kCcVj4' %}
{% set fee_receiver_4 = '7tQiiBdKoScWQkB1RmVuML7DBGnR31cuKPEtMM7Vy5SA' %}
{% set fee_receiver_5 = '4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7' %}
{% set fee_receiver_6 = '47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF' %}
{% set fee_receiver_7 = 'EMbqD9Y9jLXEa3RbCR8AsEW1kVa3EiJgDLVgvKh4qNFP' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

WITH
  botContracts AS (
    SELECT
      address
    FROM
      ethereum.creation_traces
    WHERE
      (
        "from" = 0xf414d478934c29d9a80244a3626c681a71e53bb2 -- BotDeployer1Address
        OR "from" = 0x37aAb97476bA8dC785476611006fD5dDA4eed66B -- BotDeployer2Address
      )
      AND block_time >= TIMESTAMP '2023-05-26' -- BotDeployerFirstTransactionTimestamp
  ),
  botTrades AS (
    SELECT
      dex.trades.block_time,
      amount_usd,
      IF(
        token_sold_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, -- WETHAddress
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
      dex.trades.tx_hash,
      evt_index
    FROM
      dex.trades
      JOIN botContracts ON dex.trades.tx_to = botContracts.address
    WHERE
      dex.trades.blockchain = 'ethereum'
      AND dex.trades.block_time >= TIMESTAMP '2023-05-26' -- BotDeployerFirstTransactionTimestamp
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
      ethereum.traces
      JOIN botContracts ON to = botContracts.address
    WHERE
      block_time >= TIMESTAMP '2023-05-26' -- BotDeployerFirstTransactionTimestamp
      AND value > CAST(0 AS UINT256)
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
      ethereum.traces
      JOIN botContracts ON "from" = botContracts.address
    WHERE
      block_time >= TIMESTAMP '2023-05-26' -- BotDeployerFirstTransactionTimestamp
      AND value > CAST(0 AS UINT256)
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
  ),
  minerBribes AS (
    SELECT
      tx_hash,
      SUM(deltaGwei * -1) / 1e18 AS bribeETH
    FROM
      botETHWithdrawals
      JOIN ethereum.blocks AS blocks ON (
        botETHWithdrawals.block_hash = blocks.hash
        AND botETHWithdrawals.to = blocks.miner
      )
    GROUP BY
      tx_hash
  )
SELECT
  block_time,
  block_number,
  'Ethereum' AS blockchain,
  -- Trade
  amount_usd,
  type,
  token_bought_amount,
  token_bought_symbol,
  CAST(token_bought_address AS VARCHAR) AS token_bought_address,
  token_sold_amount,
  token_sold_symbol,
  CAST(token_sold_address AS VARCHAR) AS token_sold_address,
  -- Fees
  ROUND(
    CAST(feeGwei AS DOUBLE) / CAST(depositGwei AS DOUBLE),
    /* Round feePercentage to 0.01% steps */
    4
  ) AS feePercentageFraction,
  (feeGwei / 1e18) * price AS fee_usd,
  feeGwei / 1e18 fee_token_amount,
  'ETH' AS fee_token_symbol,
  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS fee_token_address, -- WETH
  -- Bribes
  bribeETH * price AS bribe_usd,
  bribeETH AS bribe_token_amount,
  'ETH' AS bribe_token_symbol,
  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS bribe_token_address, -- WETH
  -- Dex
  project,
  version,
  token_pair,
  CAST(project_contract_address AS VARCHAR) AS project_contract_address,
  -- User
  CAST(user AS VARCHAR) AS user,
  CAST(botTrades.tx_hash AS VARCHAR) AS tx_hash,
  evt_index,
  IF(evt_index = highestEventIndex, true, false) AS isLastTradeInTransaction
FROM
  botTrades
  JOIN highestEventIndexForEachTrade ON botTrades.tx_hash = highestEventIndexForEachTrade.tx_hash
  /* Left Outer Join to support 0 fee trades */
  LEFT JOIN botETHDeltas ON botTrades.tx_hash = botETHDeltas.tx_hash
  LEFT JOIN minerBribes ON botTrades.tx_hash = minerBribes.tx_hash
  LEFT JOIN prices.usd ON (
    blockchain = 'ethereum'
    AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    AND minute = DATE_TRUNC('minute', block_time)
  )
ORDER BY
  block_time DESC,
  evt_index DESC