{{ config(
        schema = 'metrics_bitcoin'
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT 
    blockchain
    , block_date
    , case
        when substring(contract_address, 1, 3) = 'bc1' then cast(contract_address as varbinary) --we don't have bech32() function for this address type
        else from_base58(contract_address) --all other address types *should* be fine to use base58
      end as contract_address
    , symbol
    , net_transfer_amount_usd
FROM {{ source('tokens_bitcoin', 'net_transfers_daily_asset') }}