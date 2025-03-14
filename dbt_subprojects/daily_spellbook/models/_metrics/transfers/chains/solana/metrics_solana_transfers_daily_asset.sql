{{ config(
        schema = 'metrics_solana'
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
       )
}}

SELECT 
    blockchain
    , block_date
    , from_base58(contract_address) as contract_address
    , symbol
    , net_transfer_amount_usd
FROM {{ source('tokens_solana', 'net_transfers_daily_asset') }}