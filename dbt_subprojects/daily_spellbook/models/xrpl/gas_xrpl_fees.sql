{{
    config(
        schema = 'gas_xrpl',
        alias = 'fees',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_month', 'tx_hash'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
        post_hook='{{ expose_spells(\'["xrpl"]\',
                                    "sector",
                                    "gas",
                                    \'["krishhh"]\') }}'
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
xrpl_gas_fees AS (
    SELECT 
        'xrpl' AS blockchain
        ,CAST(DATE_TRUNC('month', CAST(
            PARSE_DATETIME(
                REGEXP_REPLACE(t._ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            ) AS TIMESTAMP
        )) AS DATE) AS block_month
        ,CAST(DATE_TRUNC('day', CAST(
            PARSE_DATETIME(
                REGEXP_REPLACE(t._ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            ) AS TIMESTAMP
        )) AS DATE) AS block_date
        ,CAST(
            PARSE_DATETIME(
                REGEXP_REPLACE(t._ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            ) AS TIMESTAMP
        ) AS block_time
        ,ledger_index AS block_number
        ,CAST(hash AS VARCHAR) AS tx_hash
        ,CAST(account AS VARCHAR) AS tx_from
        ,CAST(destination AS VARCHAR) AS tx_to
        ,CAST(NULL AS DOUBLE) AS gas_price
        ,CAST(NULL AS DOUBLE) AS gas_used
        ,CAST(NULL AS DOUBLE) AS gas_limit
        ,CAST(NULL AS DOUBLE) AS gas_limit_usage
        ,CAST(NULL AS DOUBLE) AS max_fee_per_gas
        ,CAST(NULL AS DOUBLE) AS priority_fee_per_gas
        ,CAST(NULL AS DOUBLE) AS max_priority_fee_per_gas
        ,CAST(NULL AS DOUBLE) AS base_fee_per_gas
        ,CAST(NULL AS DOUBLE) AS l1_fee
        ,CAST(NULL AS DOUBLE) AS l1_gas_used
        ,CAST(NULL AS DOUBLE) AS l1_gas_price
        ,CAST(NULL AS DOUBLE) AS l1_fee_scalar
        ,'XRP' AS currency_symbol
        ,CAST(fee AS DOUBLE) / 1000000.0 AS tx_fee
        ,CASE 
            WHEN p.price_usd IS NOT NULL 
            THEN (CAST(fee AS DOUBLE) / 1000000.0) * p.price_usd 
            ELSE NULL 
        END AS tx_fee_usd
        ,CAST(fee AS VARCHAR) AS tx_fee_raw
        ,ARRAY[
            CAST(
                ROW(
                    'base_fee',
                    CAST(fee AS DOUBLE) / 1000000.0
                ) AS ROW(fee_type VARCHAR, amount DOUBLE)
            )
        ] AS tx_fee_breakdown
        ,ARRAY[
            CAST(
                ROW(
                    'base_fee',
                    CASE 
                        WHEN p.price_usd IS NOT NULL 
                        THEN (CAST(fee AS DOUBLE) / 1000000.0) * p.price_usd 
                        ELSE NULL 
                    END
                ) AS ROW(fee_type VARCHAR, amount DOUBLE)
            )
        ] AS tx_fee_breakdown_usd
        ,ARRAY[
            CAST(
                ROW(
                    'base_fee',
                    CAST(fee AS VARCHAR)
                ) AS ROW(fee_type VARCHAR, amount VARCHAR)
            )
        ] AS tx_fee_breakdown_raw
        ,'rrrrrrrrrrrrrrrrrrrrrhoLvTp' AS tx_fee_currency
        ,CAST(NULL AS VARCHAR) AS block_proposer
        ,CASE 
            WHEN JSON_EXTRACT_SCALAR(metadata, '$.TransactionIndex') IS NOT NULL THEN 
                CAST(JSON_EXTRACT_SCALAR(metadata, '$.TransactionIndex') AS BIGINT)
            ELSE NULL 
        END AS tx_index
        ,transaction_type AS tx_type
        ,JSON_EXTRACT_SCALAR(metadata, '$.TransactionResult') AS transaction_result
        ,sequence
        ,1 AS evt_index
        ,'fee' AS transfer_type
        ,'native' AS token_standard
        ,CAST(account AS VARCHAR) AS from_address
        ,CAST(NULL AS VARCHAR) AS to_address
        ,'rrrrrrrrrrrrrrrrrrrrrhoLvTp' AS issuer
        ,p.price_usd
        ,fee AS fee_drops
        
    FROM {{ source('xrpl', 'transactions') }} t
    LEFT JOIN xrp_prices p ON (
        DATE_TRUNC('minute', CAST(
            PARSE_DATETIME(
                REGEXP_REPLACE(t._ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            ) AS TIMESTAMP
        )) = p.price_minute
    )
    WHERE t.transaction_type IN (
        'Payment'
        ,'PaymentChannelClaim'
        ,'CheckCash'
        ,'AMMDeposit' 
        ,'AMMWithdraw'
        ,'EscrowFinish'
    )
        AND TRY_CAST(t.fee AS DOUBLE) > 0
        {% if is_incremental() %}
        AND {{ incremental_predicate('t._ledger_close_time_human') }}
        {% endif %}
)
SELECT
    blockchain
    ,block_month
    ,block_date
    ,block_time
    ,block_number
    ,tx_hash
    ,tx_from
    ,tx_to
    ,gas_price
    ,gas_used
    ,gas_limit
    ,gas_limit_usage
    ,max_fee_per_gas
    ,priority_fee_per_gas
    ,max_priority_fee_per_gas
    ,base_fee_per_gas
    ,currency_symbol
    ,tx_fee
    ,tx_fee_usd
    ,tx_fee_raw
    ,tx_fee_breakdown
    ,tx_fee_breakdown_usd
    ,tx_fee_breakdown_raw
    ,tx_fee_currency
    ,block_proposer
    ,l1_fee
    ,l1_gas_used
    ,l1_gas_price
    ,l1_fee_scalar
    ,tx_index
    ,tx_type
    ,transaction_result
    ,sequence
    ,evt_index
    ,transfer_type
    ,token_standard
    ,from_address
    ,to_address
    ,issuer
    ,price_usd
    ,fee_drops
    
FROM xrpl_gas_fees
WHERE tx_fee IS NOT NULL 
    AND tx_fee > 0