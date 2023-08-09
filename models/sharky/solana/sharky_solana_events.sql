{{ config(
    tags=['dunesql'],
    alias = alias('events'),
    partition_by = ['block_month'],
    pre_hook = {
        'sql': '{{ set_trino_session_property(is_partitioned(model), \'join_distribution_type\', \'PARTITIONED\') }}',
        'transaction': True
    },
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'evt_type', 'loan_id', 'id'],
    post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "sharky",
                                    \'["ennnas", "hosuke"]\') }}'
    )
}}

{%- set project_start_date = '2022-04-14' %} --if testing via GH action CI tests, look to shorten this date for performance gains
{%- set sharky_smart_contract = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP' %}

WITH sharky_txs AS (
        SELECT DISTINCT tx_id AS id,
               block_time
        FROM {{ source('solana', 'account_activity') }}
        WHERE tx_success
            {% if not is_incremental() %}
            AND block_time >= TIMESTAMP '{{ project_start_date }}'
            {% else %}
            AND block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
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
            AND minute >= TIMESTAMP '{{ project_start_date }}'
            {% else %}
            AND minute >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% endif %}
    ),
    filtered_txs AS (
        SELECT signatures,
               block_date,
               block_time,
               block_slot,
               post_balances,
               pre_balances,
               account_keys,
               instructions,
               signer,
               id
        FROM {{ source('solana','transactions') }} tx
        {% if not is_incremental() %}
        WHERE tx.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% else %}
        WHERE tx.block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
        {% endif %}
    ),
    raw_events AS (
        SELECT 'solana'                                                                                     AS blockchain,
               'sharky'                                                                                     AS project,
               pre_balances,
               post_balances,
               account_keys,
               filtered_txs.signatures[1]                                                                   AS tx_hash,
               filtered_txs.block_date,
               filtered_txs.block_time,
               CAST(filtered_txs.block_slot AS BIGINT)                                                      AS block_number,
               filter(
                           filtered_txs.instructions,
                           x -> x.executing_account = '{{sharky_smart_contract}}'
                   )                                                                                        AS sharky_instructions,
               filtered_txs.signer                                                                          AS user,
               filtered_txs.id
        FROM filtered_txs
        INNER JOIN sharky_txs
            ON sharky_txs.block_time = filtered_txs.block_time
            AND sharky_txs.id = filtered_txs.id
    ),
    decoded_events AS (
        SELECT
            blockchain,
            project,
            pre_balances,
            post_balances,
            account_keys,
            tx_hash,
            block_date,
            block_time,
            block_number,
            user,
            id,
            ARRAY[sharky_instructions[rownumber]] AS sharky_instructions,
            -- We need to decode the instruction data to identify the instruction since the payload changes the base58
            -- encoded string. We start from the third character to remove the 0x prefix
            UPPER(SUBSTR(CAST(fromBase58(data) AS VARCHAR), 3)) AS ix_data
        FROM raw_events,
        UNNEST(sharky_instructions) WITH ORDINALITY AS s(data, executing_account, account_arguments, inner_ix, rownumber)
    ),
    events AS (
        SELECT *,
            CASE
                -- We are using the first 8 bytes of the instruction data to identify the instruction
               WHEN starts_with(ix_data, '2C0C4C90D2D0EF55') THEN 'Offer'
               WHEN starts_with(ix_data, '4040A0D33324B19E') THEN 'Rescind'
               WHEN (
                    starts_with(ix_data, '9935333BDE663483') -- TakeLoan
                            OR starts_with(ix_data, 'FF73DC3A1A9D70B9') -- TakeLoanV3
                        ) THEN 'Take'
               WHEN (
                            starts_with(ix_data, 'E05D904D3D118936') -- RepayLoan
                            OR starts_with(ix_data, 'BB51FA5992571440') -- RepayLoanEscrow
                            OR starts_with(ix_data, '617B55364C103D9D') -- RepayLoanV3
                        ) THEN 'Repay'
               WHEN (
                            starts_with(ix_data, 'CB5477E28901B417') -- ForecloseLoan
                            OR starts_with(ix_data, 'DAF5ED6D2ECE0D0E') -- ForecloseLoanEscrow
                            OR starts_with(ix_data, '88B8323AB75C3FD8') -- ForecloseLoanV3
                        ) THEN 'Foreclose'
               WHEN (
                            starts_with(ix_data, '02D0DEBE6D94F775') -- ExtendLoan
                            OR starts_with(ix_data, '35BC6DD273220509') -- ExtendLoanEscrow
                            OR starts_with(ix_data, '471B11834E493E5C') -- ExtendLoanV3
                        ) THEN 'Extend'
            END AS evt_type
        FROM decoded_events
), loans AS (
        SELECT *,
               CASE
                   WHEN evt_type = 'Offer' THEN
                       CASE
                           WHEN block_number > 164178914 THEN sharky_instructions[1].account_arguments[4] -- upgrade to version 3.0 2PXg25v2YGfvFCyX9najJ2mNS5QADvVYNUTtXYYhbyN3cHERqEgbnuDnVKnStaQAMa1edRhM9GAZaHYMPdR9jX8o
                           ELSE sharky_instructions[1].account_arguments[3]
                       END
                   WHEN evt_type = 'Take' THEN
                       CASE
                           WHEN block_number > 206343732 THEN sharky_instructions[1].account_arguments[5] -- upgrade to version 6.0 3Hk3RgT1aBDZAboPvjCA8XwENGaY5daCtf2AjLV2KdMgQy9ZbPju7TPdq6KFQhYhxjn6GNSaZG1ke2eQjt18tJsa
                           WHEN block_number > 184280858 THEN sharky_instructions[1].account_arguments[5] -- upgrade to version 5.0 5BjeJ7Wnf7QxHSBEhXN4htn4mECD36SfsEUZbMvzhAF5sMPMCCV1jcu2rigNV8SZohVL7J3ctLLgXZ4gVzzPkmJ7
                           WHEN block_number > 177070316 THEN sharky_instructions[1].account_arguments[6] -- upgrade to version 4.0 zFA9ffgu3Z1ETH2LdH3o6C6xzKWdJFcgA3qoXo84S6zQ2iJyaRfe61VXw6BBFGM5rFsHFm9fKSUrxEuUTy7grWR
                           WHEN block_number > 164178914 THEN sharky_instructions[1].account_arguments[7] -- upgrade to version 3.0 2PXg25v2YGfvFCyX9najJ2mNS5QADvVYNUTtXYYhbyN3cHERqEgbnuDnVKnStaQAMa1edRhM9GAZaHYMPdR9jX8o
                           WHEN block_number > 132405709 THEN sharky_instructions[1].account_arguments[6] -- upgrade to version 2.0 2oNjJSxAM72Y6t7ALKXBiMDjtwhmxEfbk8jBz4es3U6XpBfw2Jvgcu3pSvxuHUkDWbVE48xxpjwgpuxrNnPFjvxm
                           ELSE sharky_instructions[1].account_arguments[7]
                       END
                   WHEN (
                       evt_type = 'Rescind'
                       OR evt_type = 'Repay'
                       OR evt_type = 'Foreclose'
                       OR evt_type = 'Extend'
                   ) THEN sharky_instructions[1].account_arguments[1]
               END as loan_id
        FROM events
        WHERE evt_type IS NOT NULL
), final_event AS (
        SELECT *,
               CASE
                    WHEN (
                            evt_type = 'Offer'
                            OR evt_type = 'Take'
                            OR evt_type = 'Rescind'
                        ) THEN abs(post_balances[array_position(account_keys, escrow)] - pre_balances[array_position(account_keys, escrow)] )
                    ELSE (abs(post_balances[1] - pre_balances[1]))
               END AS amount_raw
        FROM (
            SELECT *,
                CASE
                    WHEN (
                            evt_type = 'Offer'
                            OR evt_type = 'Take'
                        ) THEN sharky_instructions[1].account_arguments[array_position(sharky_instructions[1].account_arguments, loan_id) + 1]
                    WHEN evt_type = 'Rescind' THEN sharky_instructions[1].account_arguments[5]
                END AS escrow
            FROM loans
        )
)
SELECT
    blockchain,
    project,
    tx_hash,
    block_date,
    CAST(date_trunc('month', block_date) AS DATE) AS block_month,
    block_time,
    block_number,
    (amount_raw / 1e9) * p.price AS amount_usd,
    amount_raw / 1e9 AS amount_original,
    CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw,
    sharky_instructions,
    user,
    id,
    evt_type,
    loan_id
FROM final_event
LEFT JOIN sol_price p
    ON p.minute = date_trunc('minute', block_time)