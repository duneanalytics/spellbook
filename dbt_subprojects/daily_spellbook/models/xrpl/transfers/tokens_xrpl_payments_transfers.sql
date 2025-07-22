{{
    config(
        schema = 'tokens_xrpl',
        alias = 'payments_transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

WITH xrp_prices AS (
    SELECT
        minute AS price_minute
        ,price AS price_usd
    FROM {{ source('prices', 'usd') }}
    WHERE symbol = 'XRP' 
        AND blockchain IS NULL
        {% if is_incremental() %}
        AND {{ incremental_predicate('minute') }}
        {% endif %}
),

successful_payment_transactions AS (
    SELECT 
        hash AS tx_hash
        ,CAST(
            DATE_TRUNC('minute', PARSE_DATETIME(
                REGEXP_REPLACE(_ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            )) AS TIMESTAMP
        ) AS block_time
        ,ledger_index
        ,account AS tx_from
        ,destination AS tx_to
        ,transaction_type
        ,sequence
        ,fee
        ,CASE 
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.TransactionIndex') IS NOT NULL THEN 
                CAST(JSON_EXTRACT_SCALAR(metadata, '$.TransactionIndex') AS BIGINT)
            ELSE NULL 
        END AS tx_index
        ,amount.currency AS amount_currency
        ,amount.issuer AS amount_issuer
        ,amount.value AS amount_value
        ,JSON_EXTRACT_SCALAR(metadata, '$.delivered_amount.currency') AS delivered_currency
        ,JSON_EXTRACT_SCALAR(metadata, '$.delivered_amount.issuer') AS delivered_issuer
        ,JSON_EXTRACT_SCALAR(metadata, '$.delivered_amount.value') AS delivered_value
        ,JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') AS transaction_result
        ,CAST(JSON_EXTRACT_SCALAR(metadata, '$.Flags') AS BIGINT) AS flags
        
    FROM {{ source('xrpl', 'transactions') }}
    WHERE transaction_type = 'Payment'
        AND JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') = 'tesSUCCESS'
        AND destination IS NOT NULL
        {% if is_incremental() %}
        AND {{ incremental_predicate('CAST(DATE_TRUNC(\'minute\', PARSE_DATETIME(REGEXP_REPLACE(_ledger_close_time_human, \' UTC$\', \'\'), \'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS\')) AS TIMESTAMP)') }}
        {% endif %}
),

payment_transfers AS (
    SELECT 
        tx_hash
        ,block_time
        ,ledger_index
        ,tx_index
        ,tx_from
        ,tx_to
        ,transaction_type
        ,transaction_result
        ,sequence
        ,fee
        ,flags
        ,COALESCE(delivered_currency, amount_currency) AS currency
        ,CASE 
            WHEN COALESCE(delivered_currency, amount_currency) = 'XRP' THEN 'rrrrrrrrrrrrrrrrrrrrrhoLvTp'
            ELSE COALESCE(delivered_issuer, amount_issuer)
        END AS issuer
        ,amount_value AS amount_requested_raw
        ,COALESCE(delivered_value, amount_value) AS amount_delivered_raw
        ,tx_from AS from_address
        ,tx_to AS to_address
        ,CASE 
            WHEN flags IS NOT NULL AND bitwise_and(flags, 131072) = 131072 THEN true
            ELSE false
        END AS partial_payment_flag
        ,CASE 
            WHEN COALESCE(delivered_currency, amount_currency) = 'XRP' THEN 'native'
            ELSE 'issued'
        END AS token_standard
        
    FROM successful_payment_transactions
    WHERE COALESCE(delivered_value, amount_value) IS NOT NULL
        AND TRY_CAST(COALESCE(delivered_value, amount_value) AS DOUBLE) > 0
),

transfers_with_amounts AS (
    SELECT
        'xrpl' AS blockchain
        ,CAST(date_trunc('month', block_time) AS DATE) AS block_month
        ,CAST(block_time AS DATE) AS block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,0 AS evt_index
        ,'direct' AS transfer_type
        ,token_standard
        ,transaction_type
        ,transaction_result
        ,sequence
        ,fee
        ,tx_from
        ,tx_to
        ,from_address
        ,to_address
        ,issuer
        ,currency
        ,CASE 
            WHEN LENGTH(currency) = 40 THEN currency
            ELSE NULL
        END AS currency_hex
        ,CASE 
            WHEN currency = 'XRP' THEN 'XRP'
            WHEN LENGTH(currency) = 40 AND cm.symbol IS NOT NULL THEN cm.symbol
            WHEN LENGTH(currency) = 40 AND cm.symbol IS NULL THEN SUBSTR(currency, 1, 8)
            ELSE currency
        END AS symbol
        ,amount_requested_raw
        ,amount_delivered_raw
        ,CASE 
            WHEN currency = 'XRP' THEN 
                TRY_CAST(amount_requested_raw AS DOUBLE) / 1000000
            ELSE 
                TRY_CAST(amount_requested_raw AS DOUBLE)
        END AS amount_requested
        ,CASE 
            WHEN currency = 'XRP' THEN 
                TRY_CAST(amount_delivered_raw AS DOUBLE) / 1000000
            ELSE 
                TRY_CAST(amount_delivered_raw AS DOUBLE)
        END AS amount_delivered
        ,partial_payment_flag
        
    FROM payment_transfers
    LEFT JOIN {{ ref('tokens_xrpl_currency_mapping') }} cm ON currency = cm.currency_hex
)

SELECT
    t.blockchain
    ,t.block_month
    ,t.block_date
    ,t.block_time
    ,t.ledger_index
    ,t.tx_hash
    ,t.tx_index
    ,t.evt_index
    ,t.transfer_type
    ,t.token_standard
    ,t.transaction_type
    ,t.transaction_result
    ,t.sequence
    ,t.fee
    ,t.tx_from
    ,t.tx_to
    ,t.from_address
    ,t.to_address
    ,t.issuer
    ,t.currency
    ,t.currency_hex
    ,t.symbol
    ,t.amount_requested_raw
    ,t.amount_delivered_raw
    ,t.amount_requested
    ,t.amount_delivered
    ,COALESCE(t.amount_delivered, t.amount_requested) AS amount
    ,t.partial_payment_flag
    ,CASE 
        WHEN t.currency = 'XRP' THEN p.price_usd
        ELSE NULL
    END AS price_usd
    ,CASE 
        WHEN t.currency = 'XRP' THEN
            COALESCE(t.amount_delivered, t.amount_requested) * COALESCE(p.price_usd, 0)
        ELSE NULL
    END AS amount_usd
    
FROM transfers_with_amounts t
LEFT JOIN xrp_prices p ON t.block_time = p.price_minute
WHERE COALESCE(t.amount_delivered, t.amount_requested) > 0