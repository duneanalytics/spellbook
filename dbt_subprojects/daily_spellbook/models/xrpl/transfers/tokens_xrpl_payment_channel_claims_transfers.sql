{{
    config(
        schema = 'tokens_xrpl',
        alias = 'payment_channel_claims_transfers',
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

successful_payment_channel_transactions AS (
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
        ,JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') AS transaction_result
        ,metadata
        
    FROM {{ source('xrpl', 'transactions') }}
    WHERE transaction_type = 'PaymentChannelClaim'
        AND JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') = 'tesSUCCESS'
        {% if is_incremental() %}
        AND {{ incremental_predicate('CAST(DATE_TRUNC(\'minute\', PARSE_DATETIME(REGEXP_REPLACE(_ledger_close_time_human, \' UTC$\', \'\'), \'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS\')) AS TIMESTAMP)') }}
        {% endif %}
),

valid_payment_channel_nodes AS (
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
        ,metadata
        ,CASE 
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[0].ModifiedNode.LedgerEntryType') = 'PayChannel' THEN 0
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[1].ModifiedNode.LedgerEntryType') = 'PayChannel' THEN 1
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[2].ModifiedNode.LedgerEntryType') = 'PayChannel' THEN 2
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[3].ModifiedNode.LedgerEntryType') = 'PayChannel' THEN 3
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[4].ModifiedNode.LedgerEntryType') = 'PayChannel' THEN 4
        END AS node_index
        
    FROM successful_payment_channel_transactions
    WHERE JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[0].ModifiedNode.LedgerEntryType') = 'PayChannel'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[1].ModifiedNode.LedgerEntryType') = 'PayChannel'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[2].ModifiedNode.LedgerEntryType') = 'PayChannel'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[3].ModifiedNode.LedgerEntryType') = 'PayChannel'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[4].ModifiedNode.LedgerEntryType') = 'PayChannel'
),

payment_channel_transfers AS (
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
        ,node_index
        -- Extract currency from PayChannel node (could be XRP or other tokens)
        ,COALESCE(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance.currency')),
            'XRP'
        ) AS currency
        -- Extract issuer for non-XRP tokens
        ,CASE 
            WHEN COALESCE(
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance.currency')),
                'XRP'
            ) = 'XRP' THEN 'rrrrrrrrrrrrrrrrrrrrrhoLvTp'
            ELSE JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance.issuer'))
        END AS issuer
        -- Calculate the balance change (amount claimed)
        ,CASE
            -- If Balance is a string (pure XRP in drops)
            WHEN JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance')) IS NOT NULL
                AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance.currency')) IS NULL
            THEN CAST(
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance'))
                AS BIGINT
            ) - CAST(
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.Balance'))
                AS BIGINT
            )
            -- If Balance is an object with value field
            ELSE CAST(
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance.value'))
                AS DOUBLE
            ) - CAST(
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.Balance.value'))
                AS DOUBLE
            )
        END AS balance_change
        -- Extract addresses - PayChannel shows the source and destination
        ,COALESCE(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Account')),
            tx_from
        ) AS from_address
        ,tx_from AS to_address  -- The account claiming the funds
        
    FROM valid_payment_channel_nodes
    WHERE node_index IS NOT NULL
        AND (
            -- Check for string format Balance (XRP)
            (JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance')) IS NOT NULL
             AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.Balance')) IS NOT NULL)
            OR 
            -- Check for object format Balance (tokens)
            (JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance.value')) IS NOT NULL
             AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.Balance.value')) IS NOT NULL)
        )
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
        ,'payment_channel_claim' AS transfer_type
        ,CASE 
            WHEN currency = 'XRP' THEN 'native'
            ELSE 'issued'
        END AS token_standard
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
        ,CAST(balance_change AS VARCHAR) AS amount_requested_raw
        ,CAST(balance_change AS VARCHAR) AS amount_delivered_raw
        ,CASE 
            WHEN currency = 'XRP' THEN 
                balance_change / 1000000.0
            ELSE 
                CAST(balance_change AS DOUBLE)
        END AS amount_requested
        ,CASE 
            WHEN currency = 'XRP' THEN 
                balance_change / 1000000.0
            ELSE 
                CAST(balance_change AS DOUBLE)
        END AS amount_delivered
        ,false AS partial_payment_flag
        
    FROM payment_channel_transfers
    LEFT JOIN {{ ref('tokens_xrpl_currency_mapping') }} cm ON currency = cm.currency_hex
    WHERE balance_change > 0  -- Only positive balance changes (actual claims)
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