{{ config(
        schema = 'metrics_ton'
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT 
  blockchain
  , block_date
  , cast(contract_address as varbinary) as contract_address
  , symbol
  , net_transfer_amount_usd
FROM {{ ref('tokens_ton_net_transfers_daily_asset') }}