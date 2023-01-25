{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "sharky",
                                    \'["ennnas"]\') }}'
    )
}}

{%- set project_start_date = '2022-04-01' %}
{%- set sharky_smart_contract = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP' %}

WITH events AS (
    SELECT
      'solana' as blockchain,
      'sharky' as project,
      signatures[0] as tx_hash,
      block_date,
      block_time,
      CAST(block_slot AS BIGINT) as block_number,
      (abs(post_balances[0] - pre_balances[0]) / 1e9) * p.price AS amount_usd,
      (abs(post_balances[0] - pre_balances[0]) / 1e9) AS amount_original,
      CAST(abs(post_balances[0] - pre_balances[0]) AS DECIMAL(38,0)) AS amount_raw,
      filter(
            instructions,
            x -> x.executing_account = '{{sharky_smart_contract}}'
          ) AS sharkyfi_instructions,
      CASE
          WHEN array_contains( log_messages, 'Program log: Instruction: OfferLoan') THEN 'Offer'
          WHEN array_contains( log_messages, 'Program log: Instruction: TakeLoan') THEN 'Take'
          WHEN array_contains( log_messages, 'Program log: Instruction: RescindLoan') THEN 'Rescind'
          WHEN array_contains( log_messages, 'Program log: Instruction: RepayLoan') THEN 'Repay'
          WHEN array_contains( log_messages, 'Program log: Instruction: ForecloseLoan') THEN 'Foreclose'
      ELSE 'Other' END as evt_type,
      signer as user,
      signatures[0] || '-' || id as unique_trade_id
    FROM {{ source('solana','transactions') }}
    LEFT JOIN prices.usd p
      ON p.minute = date_trunc('minute', block_time)
      AND p.blockchain is NULL
      AND p.symbol = 'SOL'
      {% if is_incremental() %}
      AND p.minute >= date_trunc("day", now() - interval '1 week')
      {% endif %}
    WHERE
         array_contains(account_keys, '{{sharky_smart_contract}}')
         AND success = 'True'
         {% if not is_incremental() %}
         AND block_date > '{{ project_start_date }}'
         {% endif %}
         {% if is_incremental() %}
         -- this filter will only be applied on an incremental run
         AND block_date >= date_trunc("day", now() - interval '1 week')
         {% endif %}
)
    SELECT
    *,
    CASE
        -- The smart contract was update around the 2022-12-01 and a new account was added before the loan id
        WHEN evt_type = 'Offer' THEN IF(
                sharkyfi_instructions[0].account_arguments[2] = 'So11111111111111111111111111111111111111112',
                sharkyfi_instructions[0].account_arguments[3],
                sharkyfi_instructions[0].account_arguments[2]
            )
        WHEN evt_type = 'Take' THEN IF(
                sharkyfi_instructions[0].account_arguments[4] = 'So11111111111111111111111111111111111111112',
                sharkyfi_instructions[0].account_arguments[6],
                sharkyfi_instructions[0].account_arguments[5]
            )
        WHEN (evt_type = 'Rescind' OR evt_type = 'Repay' OR evt_type = 'Foreclose') THEN sharkyfi_instructions[0].account_arguments[0]
    END as loan_id
    FROM events
