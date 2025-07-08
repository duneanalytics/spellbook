{{
    config(
        schema = 'tokens_xrpl',
        alias = 'amm_deposits_transfers',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_month', 'tx_hash', 'evt_index'],
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

successful_amm_deposit_transactions AS (
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
        -- Asset 1 (Amount field)
        ,amount.currency AS amount_currency
        ,amount.issuer AS amount_issuer
        ,amount.value AS amount_value
        -- Asset 2 (Amount2 field)
        ,amount2.currency AS amount2_currency
        ,amount2.issuer AS amount2_issuer
        ,amount2.value AS amount2_value
        ,metadata
        
    FROM {{ source('xrpl', 'transactions') }}
    WHERE transaction_type = 'AMMDeposit'
        AND JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') = 'tesSUCCESS'
        {% if is_incremental() %}
        AND {{ incremental_predicate('CAST(DATE_TRUNC(\'minute\', PARSE_DATETIME(REGEXP_REPLACE(_ledger_close_time_human, \' UTC$\', \'\'), \'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS\')) AS TIMESTAMP)') }}
        {% endif %}
),

-- Find valid AMM node indices (safe approach - no -1 indices)
valid_amm_nodes AS (
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
        ,amount_currency
        ,amount_issuer
        ,amount_value
        ,amount2_currency
        ,amount2_issuer
        ,amount2_value
        ,metadata
        ,CASE 
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[0].ModifiedNode.LedgerEntryType') = 'AMM' THEN 0
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[1].ModifiedNode.LedgerEntryType') = 'AMM' THEN 1
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[2].ModifiedNode.LedgerEntryType') = 'AMM' THEN 2
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[3].ModifiedNode.LedgerEntryType') = 'AMM' THEN 3
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[4].ModifiedNode.LedgerEntryType') = 'AMM' THEN 4
        END AS node_index
        
    FROM successful_amm_deposit_transactions
    WHERE JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[0].ModifiedNode.LedgerEntryType') = 'AMM'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[1].ModifiedNode.LedgerEntryType') = 'AMM'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[2].ModifiedNode.LedgerEntryType') = 'AMM'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[3].ModifiedNode.LedgerEntryType') = 'AMM'
        OR JSON_EXTRACT_SCALAR(metadata, '$.AffectedNodes[4].ModifiedNode.LedgerEntryType') = 'AMM'
),

-- Asset 1 Deposits (User → AMM Pool)
asset1_deposits AS (
    SELECT
        'xrpl' AS blockchain
        ,CAST(date_trunc('month', block_time) AS DATE) AS block_month
        ,CAST(block_time AS DATE) AS block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,1 AS evt_index
        ,'amm_deposit' AS transfer_type
        ,CASE WHEN amount_currency = 'XRP' THEN 'native' ELSE 'issued' END AS token_standard
        ,transaction_type
        ,transaction_result
        ,sequence
        ,fee
        ,tx_from
        ,tx_to
        ,tx_from AS from_address
        ,COALESCE(
            CASE WHEN node_index IS NOT NULL THEN
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Account'))
            END,
            'AMM_POOL'
        ) AS to_address
        ,CASE WHEN amount_currency = 'XRP' THEN 'rrrrrrrrrrrrrrrrrrrrrhoLvTp' ELSE amount_issuer END AS issuer
        ,amount_currency AS currency
        ,amount_value AS amount_requested_raw
        ,amount_value AS amount_delivered_raw
        ,CASE 
            WHEN amount_currency = 'XRP' THEN TRY_CAST(amount_value AS DOUBLE) / 1000000
            ELSE TRY_CAST(amount_value AS DOUBLE)
        END AS amount_requested
        ,CASE 
            WHEN amount_currency = 'XRP' THEN TRY_CAST(amount_value AS DOUBLE) / 1000000
            ELSE TRY_CAST(amount_value AS DOUBLE)
        END AS amount_delivered
        ,false AS partial_payment_flag
        
    FROM valid_amm_nodes
    WHERE amount_value IS NOT NULL 
        AND TRY_CAST(amount_value AS DOUBLE) > 0
),

-- Asset 2 Deposits (User → AMM Pool) - if present
asset2_deposits AS (
    SELECT
        'xrpl' AS blockchain
        ,CAST(date_trunc('month', block_time) AS DATE) AS block_month
        ,CAST(block_time AS DATE) AS block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,2 AS evt_index
        ,'amm_deposit' AS transfer_type
        ,CASE WHEN amount2_currency = 'XRP' THEN 'native' ELSE 'issued' END AS token_standard
        ,transaction_type
        ,transaction_result
        ,sequence
        ,fee
        ,tx_from
        ,tx_to
        ,tx_from AS from_address
        ,COALESCE(
            CASE WHEN node_index IS NOT NULL THEN
                JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Account'))
            END,
            'AMM_POOL'
        ) AS to_address
        ,CASE WHEN amount2_currency = 'XRP' THEN 'rrrrrrrrrrrrrrrrrrrrrhoLvTp' ELSE amount2_issuer END AS issuer
        ,amount2_currency AS currency
        ,amount2_value AS amount_requested_raw
        ,amount2_value AS amount_delivered_raw
        ,CASE 
            WHEN amount2_currency = 'XRP' THEN TRY_CAST(amount2_value AS DOUBLE) / 1000000
            ELSE TRY_CAST(amount2_value AS DOUBLE)
        END AS amount_requested
        ,CASE 
            WHEN amount2_currency = 'XRP' THEN TRY_CAST(amount2_value AS DOUBLE) / 1000000
            ELSE TRY_CAST(amount2_value AS DOUBLE)
        END AS amount_delivered
        ,false AS partial_payment_flag
        
    FROM valid_amm_nodes
    WHERE amount2_value IS NOT NULL 
        AND TRY_CAST(amount2_value AS DOUBLE) > 0
),

-- LP Token Receipt (AMM Pool → User)
lp_token_receipts AS (
    SELECT
        'xrpl' AS blockchain
        ,CAST(date_trunc('month', block_time) AS DATE) AS block_month
        ,CAST(block_time AS DATE) AS block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,3 AS evt_index
        ,'amm_lp_receive' AS transfer_type
        ,'issued' AS token_standard
        ,transaction_type
        ,transaction_result
        ,sequence
        ,fee
        ,tx_from
        ,tx_to
        ,COALESCE(
            JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.Account')),
            'AMM_POOL'
        ) AS from_address
        ,tx_from AS to_address
        ,JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.LPTokenBalance.issuer')) AS issuer
        ,JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.LPTokenBalance.currency')) AS currency
        ,CAST(
            TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.LPTokenBalance.value')) AS DOUBLE) - 
            TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.LPTokenBalance.value')) AS DOUBLE) 
            AS VARCHAR
        ) AS amount_requested_raw
        ,CAST(
            TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.LPTokenBalance.value')) AS DOUBLE) - 
            TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.LPTokenBalance.value')) AS DOUBLE) 
            AS VARCHAR
        ) AS amount_delivered_raw
        ,TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.LPTokenBalance.value')) AS DOUBLE) - 
         TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.LPTokenBalance.value')) AS DOUBLE) AS amount_requested
        ,TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.LPTokenBalance.value')) AS DOUBLE) - 
         TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.LPTokenBalance.value')) AS DOUBLE) AS amount_delivered
        ,false AS partial_payment_flag
        
    FROM valid_amm_nodes
    WHERE node_index IS NOT NULL
        AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.LPTokenBalance.value')) IS NOT NULL 
        AND JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.LPTokenBalance.value')) IS NOT NULL
        AND TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.FinalFields.LPTokenBalance.value')) AS DOUBLE) > 
            TRY_CAST(JSON_EXTRACT_SCALAR(metadata, CONCAT('$.AffectedNodes[', CAST(node_index AS VARCHAR), '].ModifiedNode.PreviousFields.LPTokenBalance.value')) AS DOUBLE)
),

-- Union all deposit transfers
all_amm_deposit_transfers AS (
    SELECT * FROM asset1_deposits
    UNION ALL
    SELECT * FROM asset2_deposits
    UNION ALL
    SELECT * FROM lp_token_receipts
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
    ,CASE 
        WHEN LENGTH(t.currency) = 40 THEN t.currency
        ELSE NULL
    END AS currency_hex
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
    
FROM all_amm_deposit_transfers t
LEFT JOIN {{ ref('tokens_xrpl_currency_mapping') }} cm 
    ON t.currency = cm.currency_hex
LEFT JOIN xrp_prices p ON t.block_time = p.price_minute
WHERE COALESCE(t.amount_delivered, t.amount_requested) > 0