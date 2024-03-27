{{ config(
    schema = 'opensea_v4_base',
    alias = 'events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0x0000a26b00c1f0df003000390027140000faa719,'opensea')
    ) as foo(wallet_address, wallet_name)
)

, trades as (
    {{ seaport_v4_trades(
     blockchain = 'base'
     ,source_transactions = source('base','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_base','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('seaport_base','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0x4200000000000000000000000000000000000006'
     ,native_token_symbol = 'ETH'
     ,start_date = '2023-07-19'
    )
  }}
)

select *
from trades
where (    fee_wallet_name = 'opensea'
           or right_hash = 0x360c6ebe
         )