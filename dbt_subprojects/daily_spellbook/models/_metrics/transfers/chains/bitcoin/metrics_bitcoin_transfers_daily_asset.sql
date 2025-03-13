{{ config(
        schema = 'metrics_bitcoin'
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT 
    blockchain
    , block_date
    , contract_address
    , symbol
    , net_transfer_amount_usd
FROM {{ source('tokens_bitcoin', 'net_transfers_daily_asset') }}