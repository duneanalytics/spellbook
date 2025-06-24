{{
    config(
        schema = 'tokens_xrpl_payment_channel_claims',
        alias = 'transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date','unique_key'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
        post_hook='{{ expose_spells(\'["xrpl"]\',
                                    "sector",
                                    "tokens",
                                    \'["krishhh"]\') }}'
    )
}}

WITH xrp_prices AS (
    SELECT
        DATE_TRUNC('minute', minute) AS price_minute
        ,AVG(price) AS price_usd
    FROM {{ source('prices', 'usd') }}
    WHERE symbol = 'XRP' 
        AND blockchain IS NULL
    GROUP BY 1
),

successful_payment_channel_transactions AS (
    SELECT 
        hash AS tx_hash
        ,CAST(ledger_close_date AS TIMESTAMP) AS block_time
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
        AND {{ incremental_predicate('ledger_close_date') }}
        {% endif %}
),

-- Find valid PayChannel node indices (safe approach - no -1 indices)
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
        -- Extract balance change from PayChannel node
        ,CAST(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance.value'))
            AS BIGINT
        ) - CAST(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.Balance.value'))
            AS BIGINT
        ) AS balance_change_drops
        -- Extract addresses
        ,COALESCE(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Account')),
            tx_from
        ) AS from_address
        ,COALESCE(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Destination')),
            tx_to
        ) AS to_address
        
    FROM valid_payment_channel_nodes
    WHERE node_index IS NOT NULL
        AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Balance.value')) IS NOT NULL
        AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.Balance.value')) IS NOT NULL
),

transfers_with_amounts AS (
    SELECT
        tx_hash || '_0' AS unique_key
        ,'xrpl' AS blockchain
        ,CAST(date_trunc('month', block_time) AS DATE) AS block_month
        ,CAST(block_time AS DATE) AS block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,0 AS evt_index
        ,'payment_channel' AS transfer_type
        ,'native' AS token_standard
        ,transaction_type
        ,transaction_result
        ,sequence
        ,fee
        ,tx_from
        ,tx_to
        ,from_address
        ,to_address
        ,NULL AS issuer
        ,'XRP' AS currency
        ,NULL AS currency_hex
        ,'XRP' AS symbol
        ,CAST(balance_change_drops AS VARCHAR) AS amount_requested_raw
        ,CAST(balance_change_drops AS VARCHAR) AS amount_delivered_raw
        ,balance_change_drops / 1000000.0 AS amount_requested
        ,balance_change_drops / 1000000.0 AS amount_delivered
        ,false AS partial_payment_flag
        
    FROM payment_channel_transfers
    WHERE balance_change_drops > 0  -- Only positive balance changes (actual transfers)
)

SELECT
    t.unique_key
    ,t.blockchain
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
LEFT JOIN xrp_prices p ON DATE_TRUNC('minute', t.block_time) = p.price_minute
WHERE COALESCE(t.amount_delivered, t.amount_requested) > 0
ORDER BY t.block_time DESC, t.tx_index