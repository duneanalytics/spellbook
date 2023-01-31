{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'id'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "sharky",
                                    \'["ennnas", "hosuke"]\') }}'
    )
}}

{%- set project_start_date = '2022-10-31' %} -- TODO change back to '2022-04-14'
{%- set sharky_smart_contract = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP' %}

WITH sharky_txs AS (
        SELECT tx_id AS id,
               block_time
        FROM {{ source('solana', 'account_activity') }}
        WHERE tx_success
        {% if not is_incremental() %}
          AND block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
          AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
          AND address = '{{sharky_smart_contract}}'
    ),
    sol_price AS (
        SELECT minute,
               price
        FROM {{ source('prices', 'usd') }}
        WHERE
        blockchain IS NULL
        AND symbol = 'SOL'
        {% if not is_incremental() %}
        AND minute >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        AND minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ),
    filtered_txs AS (
        SELECT *
        FROM {{ source('solana','transactions') }} tx
        {% if not is_incremental() %}
        WHERE tx.block_time >= '{{ project_start_date }}'
        {% endif %}
        {% if is_incremental() %}
        WHERE tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ),
    events AS (
        SELECT 'solana'                                                        AS blockchain,
               'sharky'                                                        AS project,
               signatures[0]                                                   AS tx_hash,
               block_date,
               block_time,
               CAST(block_slot AS BIGINT)                                      AS block_number,
               (abs(post_balances[0] - pre_balances[0]) / 1e9) * p.price       AS amount_usd,
               (abs(post_balances[0] - pre_balances[0]) / 1e9)                 AS amount_original,
               CAST(abs(post_balances[0] - pre_balances[0]) AS DECIMAL(38, 0)) AS amount_raw,
               filter(
                           instructions,
                           x -> x.executing_account = '{{sharky_smart_contract}}'
                   )                                                           AS sharky_instructions,
               CASE
                   WHEN array_contains(log_messages, 'Program log: Instruction: OfferLoan') THEN 'Offer'
                   WHEN array_contains(log_messages, 'Program log: Instruction: TakeLoan') THEN 'Take'
                   WHEN array_contains(log_messages, 'Program log: Instruction: RescindLoan') THEN 'Rescind'
                   WHEN
                       (
                               array_contains(log_messages, 'Program log: Instruction: RepayLoan')
                               OR array_contains(log_messages, 'Program log: Instruction: RepayLoanEscrow')
                           ) THEN 'Repay'
                   WHEN
                       (
                               array_contains(log_messages, 'Program log: Instruction: ForecloseLoan')
                               OR array_contains(log_messages, 'Program log: Instruction: ForecloseLoanEscrow')
                           ) THEN 'Foreclose'
                   ELSE 'Other' END                                            AS evt_type,
               signer                                                          AS user,
               id
        FROM sharky_txs
        INNER JOIN filtered_txs USING (block_time, id)
        LEFT JOIN sol_price p
            ON p.minute = date_trunc('minute', block_time)
)
SELECT *,
       CASE
           -- The smart contract was update around the 2022-12-01 and a new account was added before the loan id
           WHEN evt_type = 'Offer' THEN IF(
                       sharky_instructions[0].account_arguments[2] = 'So11111111111111111111111111111111111111112',
                       sharky_instructions[0].account_arguments[3],
                       sharky_instructions[0].account_arguments[2]
               )
           WHEN evt_type = 'Take' THEN IF(
                       sharky_instructions[0].account_arguments[4] = 'So11111111111111111111111111111111111111112',
                       sharky_instructions[0].account_arguments[6],
                       sharky_instructions[0].account_arguments[5]
               )
           WHEN (evt_type = 'Rescind' OR evt_type = 'Repay' OR evt_type = 'Foreclose')
               THEN sharky_instructions[0].account_arguments[0]
           END as loan_id
FROM events
;