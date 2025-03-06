{{ config(
        schema = 'tokens_bitcoin'
        , alias = 'net_transfers_daily_asset'
        , materialized = 'view'
)
}}

select
    blockchain
    , block_date
    , '0x0000000000000000000000000000000000000000' as contract_address
    , 'BTC' as symbol
    , net_transfer_amount_usd
from
    {{ ref('tokens_bitcoin_net_transfers_daily') }}
