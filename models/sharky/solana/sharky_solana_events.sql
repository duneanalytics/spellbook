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

{%- set project_start_date = '2022-04-14' %} --if testing via GH action CI tests, look to shorten this date for performance gains
{%- set sharky_smart_contract = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP' %}

WITH sharky_txs AS (
        SELECT tx_id AS id,
               block_time
        FROM {{ source('solana', 'account_activity') }}
        WHERE tx_success
            {% if not is_incremental() %}
            AND block_time >= '{{ project_start_date }}'
            {% else %}
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
            {% else %}
            AND minute >= date_trunc("day", now() - interval '1 week')
            {% endif %}
    ),
    filtered_txs AS (
        SELECT signatures,
               block_date,
               block_time,
               block_slot,
               post_balances,
               pre_balances,
               instructions,
               signer,
               id
        FROM {{ source('solana','transactions') }} tx
        {% if not is_incremental() %}
        WHERE tx.block_time >= '{{ project_start_date }}'
        {% else %}
        WHERE tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    ),
    raw_events AS (
        SELECT 'solana'                                                                                     AS blockchain,
               'sharky'                                                                                     AS project,
               filtered_txs.signatures[0]                                                                   AS tx_hash,
               filtered_txs.block_date,
               filtered_txs.block_time,
               CAST(filtered_txs.block_slot AS BIGINT)                                                      AS block_number,
               (abs(filtered_txs.post_balances[0] - filtered_txs.pre_balances[0]) / 1e9) * p.price          AS amount_usd,
               (abs(filtered_txs.post_balances[0] - filtered_txs.pre_balances[0]) / 1e9)                    AS amount_original,
               CAST(abs(filtered_txs.post_balances[0] - filtered_txs.pre_balances[0]) AS DECIMAL(38, 0))    AS amount_raw,
               filter(
                           filtered_txs.instructions,
                           x -> x.executing_account = '{{sharky_smart_contract}}'
                   )                                                                                        AS sharky_instructions,
               filtered_txs.signer                                                                          AS user,
               filtered_txs.id
        FROM sharky_txs
        INNER JOIN filtered_txs
            ON sharky_txs.block_time = filtered_txs.block_time
            AND sharky_txs.id = filtered_txs.id
        LEFT JOIN sol_price p
            ON p.minute = date_trunc('minute', filtered_txs.block_time)
    ),
    decoded_events AS (
        SELECT *,
            -- We need to decode the instruction data to identify the instruction since the payload changes the base58 encoded string
            base58_decode(sharky_instructions[0].data) AS first_ix_data
        FROM raw_events
    ),
    events AS (
        SELECT blockchain,
            project,
            tx_hash,
            block_date,
            block_time,
            block_number,
            amount_usd,
            amount_original,
            amount_raw,
            sharky_instructions,
            user,
            id,
            CASE
                -- We are using the first 8 bytes of the instruction data to identify the instruction
               WHEN startswith(first_ix_data, '2C0C4C90D2D0EF55') THEN 'Offer'
               WHEN startswith(first_ix_data, '4040A0D33324B19E') THEN 'Rescind'
               WHEN startswith(first_ix_data, '9935333BDE663483') THEN 'Take'
               WHEN (
                            startswith(first_ix_data, 'E05D904D3D118936') -- RepayLoan
                            OR startswith(first_ix_data, 'BB51FA5992571440') -- RepayLoanEscrow
                        ) THEN 'Repay'
               WHEN (
                            startswith(first_ix_data, 'CB5477E28901B417') -- ForecloseLoan
                            OR startswith(first_ix_data, 'DAF5ED6D2ECE0D0E') -- ForecloseLoanEscrow
                        ) THEN 'Foreclose'
            END AS evt_type
    FROM decoded_events
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