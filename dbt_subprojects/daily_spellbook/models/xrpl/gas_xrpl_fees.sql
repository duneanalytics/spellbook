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
            DATE_TRUNC('minute', PARSE_DATETIME(
                REGEXP_REPLACE(_ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            )) AS TIMESTAMP
        )) AS DATE) AS block_month
        ,CAST(DATE_TRUNC('day', CAST(
            DATE_TRUNC('minute', PARSE_DATETIME(
                REGEXP_REPLACE(_ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            )) AS TIMESTAMP
        )) AS DATE) AS block_date
        ,CAST(
            DATE_TRUNC('minute', PARSE_DATETIME(
                REGEXP_REPLACE(_ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            )) AS TIMESTAMP
        ) AS block_time
        ,ledger_index AS block_number
        ,CAST(hash AS VARCHAR) AS tx_hash
        ,CAST(account AS VARCHAR) AS tx_from
        ,CAST(destination AS VARCHAR) AS tx_to
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
            ELSE CAST(NULL AS BIGINT)
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
            DATE_TRUNC('minute', PARSE_DATETIME(
                REGEXP_REPLACE(_ledger_close_time_human, ' UTC$', ''),
                'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS'
            )) AS TIMESTAMP
        )) = p.price_minute
    )
    WHERE transaction_type IN (
        'Payment'
        ,'PaymentChannelClaim'
        ,'CheckCash'
        ,'AMMDeposit' 
        ,'AMMWithdraw'
        ,'EscrowFinish'
    )
        AND TRY_CAST(fee AS DOUBLE) > 0
        {% if is_incremental() %}
        AND {{ incremental_predicate('CAST(DATE_TRUNC(\'minute\', PARSE_DATETIME(REGEXP_REPLACE(_ledger_close_time_human, \' UTC$\', \'\'), \'yyyy-MMM-dd HH:mm:ss.SSSSSSSSS\')) AS TIMESTAMP)') }}
        {% endif %}
)
SELECT
    *
    
FROM xrpl_gas_fees
WHERE tx_fee IS NOT NULL 
    AND tx_fee > 0