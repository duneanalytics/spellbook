{{
    config(
        schema = 'tokens_xrpl',
        alias = 'transfers',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["xrpl"]\',
                                    "sector",
                                    "tokens",
                                    \'["krishhh"]\') }}'
    )
}}


WITH all_transfers AS (
    -- 1. Payment Transfers
    SELECT 
        unique_key
        ,blockchain
        ,block_month
        ,block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,evt_index
        ,transfer_type
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
        ,currency_hex
        ,symbol
        ,amount_requested_raw
        ,amount_delivered_raw
        ,amount_requested
        ,amount_delivered
        ,amount
        ,partial_payment_flag
        ,price_usd
        ,amount_usd
    FROM {{ ref('tokens_xrpl_payments') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
    
    UNION ALL
    
    -- 2. Payment Channel Claim Transfers
    SELECT 
        unique_key
        ,blockchain
        ,block_month
        ,block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,evt_index
        ,transfer_type
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
        ,currency_hex
        ,symbol
        ,amount_requested_raw
        ,amount_delivered_raw
        ,amount_requested
        ,amount_delivered
        ,amount
        ,partial_payment_flag
        ,price_usd
        ,amount_usd
    FROM {{ ref('tokens_xrpl_payment_channel_claims') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
    
    UNION ALL
    
    -- 3. Check Cash Transfers
    SELECT 
        unique_key
        ,blockchain
        ,block_month
        ,block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,evt_index
        ,transfer_type
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
        ,currency_hex
        ,symbol
        ,amount_requested_raw
        ,amount_delivered_raw
        ,amount_requested
        ,amount_delivered
        ,amount
        ,partial_payment_flag
        ,price_usd
        ,amount_usd
    FROM {{ ref('tokens_xrpl_check_cash') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
    
    UNION ALL
    
    -- 4. Escrow Finish Transfers
    SELECT 
        unique_key
        ,blockchain
        ,block_month
        ,block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,evt_index
        ,transfer_type
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
        ,currency_hex
        ,symbol
        ,amount_requested_raw
        ,amount_delivered_raw
        ,amount_requested
        ,amount_delivered
        ,amount
        ,partial_payment_flag
        ,price_usd
        ,amount_usd
    FROM {{ ref('tokens_xrpl_escrow_finish') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
    
    UNION ALL
    
    -- 5. AMM Deposit Transfers (Multiple transfers per transaction)
    SELECT 
        unique_key
        ,blockchain
        ,block_month
        ,block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,evt_index
        ,transfer_type
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
        ,currency_hex
        ,symbol
        ,amount_requested_raw
        ,amount_delivered_raw
        ,amount_requested
        ,amount_delivered
        ,amount
        ,partial_payment_flag
        ,price_usd
        ,amount_usd
    FROM {{ ref('tokens_xrpl_amm_deposits') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
    
    UNION ALL
    
    -- 6. AMM Withdraw Transfers (Multiple transfers per transaction)
    SELECT 
        unique_key
        ,blockchain
        ,block_month
        ,block_date
        ,block_time
        ,ledger_index
        ,tx_hash
        ,tx_index
        ,evt_index
        ,transfer_type
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
        ,currency_hex
        ,symbol
        ,amount_requested_raw
        ,amount_delivered_raw
        ,amount_requested
        ,amount_delivered
        ,amount
        ,partial_payment_flag
        ,price_usd
        ,amount_usd
    FROM {{ ref('tokens_xrpl_amm_withdraws') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
)

SELECT
    unique_key
    ,blockchain
    ,block_month
    ,block_date
    ,block_time
    ,ledger_index
    ,tx_hash
    ,tx_index
    ,evt_index
    ,transfer_type
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
    ,currency_hex
    ,symbol
    ,amount_requested_raw
    ,amount_delivered_raw
    ,amount_requested
    ,amount_delivered
    ,amount
    ,partial_payment_flag
    ,price_usd
    ,amount_usd
    
FROM all_transfers
WHERE amount > 0
ORDER BY block_time DESC, tx_hash, evt_index