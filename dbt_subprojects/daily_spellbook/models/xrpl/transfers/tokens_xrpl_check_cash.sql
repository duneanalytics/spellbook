{{
    config(
        schema = 'tokens_xrpl_check_cash',
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

successful_check_cash_transactions AS (
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
        ,JSON_EXTRACT_SCALAR(metadata, '$.delivered_amount.currency') AS delivered_currency
        ,JSON_EXTRACT_SCALAR(metadata, '$.delivered_amount.issuer') AS delivered_issuer
        ,JSON_EXTRACT_SCALAR(metadata, '$.delivered_amount.value') AS delivered_value
        ,JSON_EXTRACT_SCALAR(metadata, '$.delivered_amount') AS delivered_amount_xrp
        ,metadata
        
    FROM {{ source('xrpl', 'transactions') }}
    WHERE transaction_type = 'CheckCash'
        AND JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') = 'tesSUCCESS'
        {% if is_incremental() %}
        AND {{ incremental_predicate('ledger_close_date') }}
        {% endif %}
),

-- Find valid Check node indices (safe approach - no -1 indices)
valid_check_nodes AS (
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
        ,delivered_currency
        ,delivered_issuer
        ,delivered_value
        ,delivered_amount_xrp
        ,metadata
        ,CASE 
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[0].DeletedNode.LedgerEntryType') = 'Check' THEN 0
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[1].DeletedNode.LedgerEntryType') = 'Check' THEN 1
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[2].DeletedNode.LedgerEntryType') = 'Check' THEN 2
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[3].DeletedNode.LedgerEntryType') = 'Check' THEN 3
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[4].DeletedNode.LedgerEntryType') = 'Check' THEN 4
        END AS node_index
        
    FROM successful_check_cash_transactions
    WHERE JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[0].DeletedNode.LedgerEntryType') = 'Check'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[1].DeletedNode.LedgerEntryType') = 'Check'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[2].DeletedNode.LedgerEntryType') = 'Check'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[3].DeletedNode.LedgerEntryType') = 'Check'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[4].DeletedNode.LedgerEntryType') = 'Check'
        OR (delivered_currency IS NOT NULL OR delivered_value IS NOT NULL OR delivered_amount_xrp IS NOT NULL)
),

check_cash_transfers AS (
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
        -- Get currency info from Check node or delivered_amount
        ,COALESCE(
            CASE WHEN node_index IS NOT NULL THEN
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.SendMax.currency'))
            END,
            delivered_currency,
            'XRP'
        ) AS currency
        -- Get issuer info
        ,CASE 
            WHEN COALESCE(
                CASE WHEN node_index IS NOT NULL THEN
                    JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.SendMax.currency'))
                END,
                delivered_currency,
                'XRP'
            ) = 'XRP' THEN NULL
            ELSE COALESCE(
                CASE WHEN node_index IS NOT NULL THEN
                    JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.SendMax.issuer'))
                END,
                delivered_issuer
            )
        END AS issuer
        -- Get amount value
        ,COALESCE(
            CASE WHEN node_index IS NOT NULL THEN
                COALESCE(
                    JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.SendMax')),
                    JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.SendMax.value'))
                )
            END,
            delivered_amount_xrp,
            delivered_value
        ) AS amount_raw
        -- From and To addresses
        ,COALESCE(
            CASE WHEN node_index IS NOT NULL THEN
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].DeletedNode.FinalFields.Account'))
            END,
            tx_from
        ) AS from_address
        ,tx_from AS to_address  -- Check casher receives the funds
        
    FROM valid_check_nodes
    WHERE (node_index IS NOT NULL OR delivered_value IS NOT NULL OR delivered_amount_xrp IS NOT NULL)
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
        ,'check_cash' AS transfer_type
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
            WHEN LENGTH(currency) = 40 THEN 
                CASE 
                    WHEN SUBSTR(currency, 1, 16) = '586F676500000000' THEN 'Xoge'
                    WHEN SUBSTR(currency, 1, 16) = '5363686D65636B6C' THEN 'Schmeckles' 
                    WHEN SUBSTR(currency, 1, 16) = '524C555344000000' THEN 'RLUSD'
                    WHEN SUBSTR(currency, 1, 16) = '5349474D41000000' THEN 'SIGMA'
                    WHEN SUBSTR(currency, 1, 16) = '5348524F4F4D4945' THEN 'SHROOMIES'
                    WHEN SUBSTR(currency, 1, 16) = '4841494300000000' THEN 'HAIC'
                    WHEN SUBSTR(currency, 1, 16) = '4249545800000000' THEN 'BITX'
                    WHEN SUBSTR(currency, 1, 16) = '515A696C6C610000' THEN 'QZilla'
                    WHEN SUBSTR(currency, 1, 16) = '5852505400000000' THEN 'XRPT'
                    WHEN SUBSTR(currency, 1, 16) = '584D454D45000000' THEN 'XMEME'
                    WHEN SUBSTR(currency, 1, 16) = '4752554D50590000' THEN 'GRUMPY'
                    WHEN SUBSTR(currency, 1, 16) = '6277696600000000' THEN 'bwif'
                    WHEN SUBSTR(currency, 1, 16) = '534F4C4F00000000' THEN 'SOLO'
                    WHEN SUBSTR(currency, 1, 16) = '4D4F4F4E00000000' THEN 'MOON'
                    WHEN SUBSTR(currency, 1, 16) = '52495A5A4C450000' THEN 'RIZZLE'
                    WHEN SUBSTR(currency, 1, 16) = '534B554C4C000000' THEN 'SKULL'
                    WHEN SUBSTR(currency, 1, 16) = '5852507300000000' THEN 'XRPs'
                    WHEN SUBSTR(currency, 1, 16) = '52454D4F00000000' THEN 'REMO'
                    WHEN SUBSTR(currency, 1, 16) = '5354494D50594348' THEN 'STIMPYCHA'
                    ELSE SUBSTR(currency, 1, 8)
                END
            ELSE currency
        END AS symbol
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
        
    FROM check_cash_transfers
    WHERE amount_raw IS NOT NULL
        AND TRY_CAST(amount_raw AS DOUBLE) > 0
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