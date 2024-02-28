{{ config(
    
    schema = 'sharky_solana',
    alias = 'events',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'evt_type', 'loan_id', 'id'],
    pre_hook='{{ enforce_join_distribution("PARTITIONED") }}',
    post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "sharky",
                                    \'["ennnas", "hosuke"]\') }}'
    )
}}

{%- set project_start_date = '2022-04-14' %} --if testing via GH action CI tests, look to shorten this date for performance gains
{%- set sharky_smart_contract = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP' %}

WITH
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
    offers AS (
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Offer' AS evt_type,
            account_escrow,
            account_orderBook,
            NULL AS account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions,
            CAST(principalLamports AS BIGINT) AS amount_raw
        FROM {{ source('sharky_solana', 'sharky_call_offerLoan') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}
    ), rescind AS (
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Rescind' AS evt_type,
            account_escrow,
            NULL AS account_orderBook,
            NULL AS account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_rescindLoan') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}
    ),
    take AS (
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Take' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_takeLoan') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Take' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_takeLoanV3') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Take' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_takeLoanV3Compressed') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}
    ), repay AS (
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Repay' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_repayLoan') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Repay' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_repayLoanEscrow') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Repay' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_repayLoanV3') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Repay' AS evt_type,
            account_escrow,
            account_orderBook,
            NULL AS account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_repayLoanV3Compressed') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}
    ), foreclose AS (
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Foreclose' AS evt_type,
            account_escrow,
            NULL AS account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_forecloseLoan') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Foreclose' AS evt_type,
            account_escrow,
            NULL AS account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_forecloseLoanEscrow') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Foreclose' AS evt_type,
            account_escrow,
            NULL AS account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_forecloseLoanV3') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Foreclose' AS evt_type,
            account_escrow,
            NULL AS account_orderBook,
            NULL AS account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_forecloseLoanV3Compressed') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}
    ), extend AS (
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Extend' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_extendLoan') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Extend' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_extendLoanEscrow') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Extend' AS evt_type,
            account_escrow,
            account_orderBook,
            account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_extendLoanV3') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}

        UNION ALL
        SELECT
            call_block_date,
            call_block_slot,
            call_block_time,
            call_tx_signer,
            call_tx_id,
            account_loan,
            'Extend' AS evt_type,
            account_escrow,
            account_orderBook,
            NULL AS account_collateralMint,
            ARRAY[CAST(ROW(call_data, call_account_arguments) AS ROW(data VARBINARY, account_arguments ARRAY<VARCHAR>))] AS sharky_instructions
        FROM {{ source('sharky_solana', 'sharky_call_extendLoanV3Compressed') }}
        WHERE
            {% if is_incremental() %}
            call_block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
            {% else %}
            True
            {% endif %}
), with_amount AS (
    SELECT
       events.*,
       CASE
            -- For take and rescind instructions we can take the exact amount available in the escrow account
            WHEN (
                    events.evt_type = 'Take'
                    OR events.evt_type = 'Rescind'
                ) THEN abs(st.post_balances[array_position(st.account_keys, events.account_escrow)] - st.pre_balances[array_position(st.account_keys, events.account_escrow)] )
            ELSE (abs(st.post_balances[1] - st.pre_balances[1]))
       END AS amount_raw
    FROM  {{ source('solana','transactions') }} st
    INNER JOIN (
        SELECT * FROM take
        UNION ALL
        SELECT * FROM rescind
        UNION ALL
        SELECT * FROM repay
        UNION ALL
        SELECT * FROM extend
        UNION ALL
        SELECT * FROM foreclose
    ) events ON (events.call_block_time = st.block_time AND events.call_tx_id = st.id)
    WHERE
        True
        {% if not is_incremental() %}
        AND st.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% else %}
        AND st.block_time >= DATE_TRUNC('day', NOW() - INTERVAL '7' DAY)
        {% endif %}
), final_event AS (
    SELECT * FROM offers
    UNION ALL
    SELECT * FROM with_amount
)
SELECT
    'solana' AS blockchain,
    'sharky' AS project,
    call_tx_id AS tx_hash,
    call_block_date AS block_date,
    CAST(date_trunc('month', call_block_date) AS DATE) AS block_month,
    call_block_time AS block_time,
    call_block_slot AS block_number,
    (amount_raw / 1e9) * p.price AS amount_usd,
    amount_raw / 1e9 AS amount_original,
    CAST(amount_raw AS DECIMAL(38, 0)) AS amount_raw,
    sharky_instructions,
    call_tx_signer AS user,
    call_tx_id AS id,
    evt_type,
    account_loan AS loan_id,
    account_orderBook AS orderbook,
    account_collateralMint AS nft
FROM final_event
LEFT JOIN sol_price p ON p.minute = date_trunc('minute', call_block_time)
