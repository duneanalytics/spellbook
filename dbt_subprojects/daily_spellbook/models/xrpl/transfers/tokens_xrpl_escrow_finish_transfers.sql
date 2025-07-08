{{
    config(
        schema = 'tokens_xrpl',
        alias = 'escrow_finish_transfers',
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

successful_escrow_finish_transactions AS (
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
            ELSE CAST(NULL AS BIGINT)
        END AS tx_index
        ,JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') AS transaction_result
        ,metadata
        
    FROM {{ source('xrpl', 'transactions') }}
    WHERE transaction_type = 'EscrowFinish'
        AND JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') = 'tesSUCCESS'
        {% if is_incremental() %}
        AND {{ incremental_predicate('CAST(DATE_TRUNC(\'minute\', PARSE_DATETIME(REGEXP_REPLACE(_ledger_close_time_human, \' UTC$\', \'\'), \'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS\')) AS TIMESTAMP)') }}
        {% endif %}
),

-- Find valid Escrow node indices (safe approach - no -1 indices)
valid_escrow_nodes AS (
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
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[0].DeletedNode.LedgerEntryType') = 'Escrow' THEN 0
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[1].DeletedNode.LedgerEntryType') = 'Escrow' THEN 1
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[2].DeletedNode.LedgerEntryType') = 'Escrow' THEN 2
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[3].DeletedNode.LedgerEntryType') = 'Escrow' THEN 3
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[4].DeletedNode.LedgerEntryType') = 'Escrow' THEN 4
        END AS node_index
        
    FROM successful_escrow_finish_transactions
    WHERE JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[0].DeletedNode.LedgerEntryType') = 'Escrow'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[1].DeletedNode.LedgerEntryType') = 'Escrow'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[2].DeletedNode.LedgerEntryType') = 'Escrow'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[3].DeletedNode.LedgerEntryType') = 'Escrow'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[4].DeletedNode.LedgerEntryType') = 'Escrow'
),

escrow_finish_transfers AS (
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
        -- Handle both string and object Amount formats
        ,CASE
            -- If Amount is a string (pure XRP in drops)
            WHEN JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount')) IS NOT NULL
                AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount.currency')) IS NULL
            THEN 'XRP'
            -- If Amount is an object with currency field
            ELSE COALESCE(
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount.currency')),
                'XRP'
            )
        END AS currency
        -- Get issuer (always NULL for XRP)
        ,CASE 
            WHEN CASE
                WHEN JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount')) IS NOT NULL
                    AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount.currency')) IS NULL
                THEN 'XRP'
                ELSE COALESCE(
                    JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount.currency')),
                    'XRP'
                )
            END = 'XRP' THEN 'rrrrrrrrrrrrrrrrrrrrrhoLvTp'
            ELSE JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount.issuer'))
        END AS issuer
        -- Get amount value - handle both string and object formats
        ,CASE
            -- If Amount is a string (pure XRP in drops)
            WHEN JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount')) IS NOT NULL
                AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount.currency')) IS NULL
            THEN JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount'))
            -- If Amount is an object with value field
            ELSE JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount.value'))
        END AS amount_raw
        -- Get addresses from the escrow node
        ,COALESCE(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Account')),
            tx_from
        ) AS from_address
        ,COALESCE(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Destination')),
            tx_to
        ) AS to_address
        
    FROM valid_escrow_nodes
    WHERE node_index IS NOT NULL
        AND (
            -- Check for string format Amount
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount')) IS NOT NULL
            OR 
            -- Check for object format Amount
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Amount.value')) IS NOT NULL
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
        ,'escrow_finish' AS transfer_type
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
            ELSE CAST(NULL AS VARCHAR)
        END AS currency_hex
        ,currency AS symbol
        ,amount_raw AS amount_requested_raw
        ,amount_raw AS amount_delivered_raw
        ,CASE 
            WHEN currency = 'XRP' THEN 
                TRY_CAST(amount_raw AS DOUBLE) / 1000000
            ELSE 
                TRY_CAST(amount_raw AS DOUBLE)
        END AS amount_requested
        ,CASE 
            WHEN currency = 'XRP' THEN 
                TRY_CAST(amount_raw AS DOUBLE) / 1000000
            ELSE 
                TRY_CAST(amount_raw AS DOUBLE)
        END AS amount_delivered
        ,false AS partial_payment_flag
        
    FROM escrow_finish_transfers
    WHERE amount_raw IS NOT NULL
        AND TRY_CAST(amount_raw AS DOUBLE) > 0
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
    ,CASE 
        WHEN t.currency = 'XRP' THEN 'XRP'
        WHEN LENGTH(t.currency) = 40 AND cm.symbol IS NOT NULL THEN cm.symbol
        WHEN LENGTH(t.currency) = 40 AND cm.symbol IS NULL THEN SUBSTR(t.currency, 1, 8)
        ELSE t.currency
    END AS symbol
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
LEFT JOIN {{ ref('tokens_xrpl_currency_mapping') }} cm 
    ON t.currency = cm.currency_hex
LEFT JOIN xrp_prices p ON t.block_time = p.price_minute
WHERE COALESCE(t.amount_delivered, t.amount_requested) > 0