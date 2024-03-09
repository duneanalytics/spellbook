{{ config(
    alias = 'trades',
    schema = 'bonkbot_solana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'tx_id', 'tx_index', 'outer_instruction_index', 'token_bought_address', 'token_sold_address'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "dex",
                                \'["whale_hunter"]\') }}'
    )
}}

{% set project_start_date = '2023-08-17' %}
{% set fee_receiver = 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

WITH
  allFeePayments AS (
    SELECT
      tx_id,
      IF(balance_change > 0, 'SOL', 'SPL') AS feeTokenType,
      IF(
        balance_change > 0,
        balance_change / 1e9,
        token_balance_change
      ) AS fee_token_amount,
      IF(
        balance_change > 0,
        '{{wsol_token}}',
        token_mint_address
      ) AS fee_token_mint_address
    FROM
      {{ source('solana','account_activity') }}
    WHERE
      block_time >= TIMESTAMP '{{project_start_date}}'
      AND tx_success
      AND (
        (
          address = '{{fee_receiver}}'
          AND balance_change > 0 -- SOL fee payments
        )
        OR (
          token_balance_owner = '{{fee_receiver}}'
          AND token_balance_change > 0 -- SPL fee payments
        )
      )
  ),
  botTrades AS (
    SELECT
      block_time,
      DATE_TRUNC('day', block_time) AS block_date,
      DATE_TRUNC('month', block_time) AS block_month,
      'Solana' AS blockchain,
      amount_usd,
      IF(
        token_sold_mint_address = '{{wsol_token}}',
        'Buy',
        'Sell'
      ) AS type,
      token_bought_amount,
      token_bought_symbol,
      token_bought_mint_address AS token_bought_address,
      token_sold_amount,
      token_sold_symbol,
      token_sold_mint_address AS token_sold_address,
      fee_token_amount * price AS fee_usd,
      fee_token_amount,
      IF(feeTokenType = 'SOL', 'SOL', symbol) AS fee_token_symbol,
      fee_token_mint_address AS fee_token_address,
      project,
      version,
      token_pair,
      project_program_id AS project_contract_address,
      trader_id AS user,
      trades.tx_id,
      tx_index,
      outer_instruction_index,
      inner_instruction_index
    FROM
      {{ ref('dex_solana_trades') }} AS trades
      JOIN allFeePayments AS feePayments ON trades.tx_id = feePayments.tx_id
      LEFT JOIN {{ source('prices', 'usd') }} AS feeTokenPrices ON (
        feeTokenPrices.blockchain = 'solana'
        AND fee_token_mint_address = toBase58 (feeTokenPrices.contract_address)
        AND date_trunc('minute', block_time) = minute
        AND minute >= TIMESTAMP '{{project_start_date}}'
      )
    WHERE
      trades.block_time >= TIMESTAMP '{{project_start_date}}'
      AND trades.trader_id != '{{fee_receiver}}' -- Exclude trades signed by FeeWallet
  ),
  highestInnerInstructionIndexForEachTrade AS (
    SELECT
      tx_id,
      outer_instruction_index,
      MAX(inner_instruction_index) AS highestInnerInstructionIndex
    FROM
      botTrades
    GROUP BY
      tx_id,
      outer_instruction_index
  )
SELECT
  block_time,
  block_date,
  block_month,
  blockchain,
  amount_usd,
  type,
  token_bought_amount,
  token_bought_symbol,
  token_bought_address,
  token_sold_amount,
  token_sold_symbol,
  token_sold_address,
  fee_usd,
  fee_token_amount,
  fee_token_symbol,
  fee_token_address,
  project,
  version,
  token_pair,
  project_contract_address,
  user,
  botTrades.tx_id,
  tx_index,
  botTrades.outer_instruction_index,
  inner_instruction_index,
  IF(
    inner_instruction_index = highestInnerInstructionIndex,
    true,
    false
  ) AS is_last_trade_in_transaction
FROM
  botTrades
  JOIN highestInnerInstructionIndexForEachTrade ON (
    botTrades.tx_id = highestInnerInstructionIndexForEachTrade.tx_id
    AND botTrades.outer_instruction_index = highestInnerInstructionIndexForEachTrade.outer_instruction_index
  )
ORDER BY
  block_time DESC,
  tx_index DESC,
  outer_instruction_index DESC,
  inner_instruction_index DESC