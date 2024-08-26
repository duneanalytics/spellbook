{{ config(
    schema = 'opensea_v4_nova',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'sub_tx_trade_id']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values   (0x5b3256965e7c3cf26e11fcaf296dfc8807c01073,'opensea')
            ,(0x8de9c5a032463c561423387a9648c5c7bcc5bc90,'opensea')
            ,(0x34ba0f2379bf9b81d09f7259892e26a8b0885095,'opensea')
            ,(0x0000a26b00c1f0df003000390027140000faa719,'opensea')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v4_trades(
     blockchain = 'nova'
     ,source_transactions = source('nova','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_nova','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('seaport_nova','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2023-01-05'
    )
  }}
)

select *
from trades
where
( zone_address in (0xf397619df7bfd4d1657ea9bdd9df7ff888731a11
          ,0x9b814233894cd227f561b78cc65891aa55c62ad2
          ,0x004c00500000ad104d7dbd00e3ae0a5c00560c00
          ,0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd
          ,0x000000e7ec00e7b300774b00001314b8610022b8 -- newly added on seaport v1.4
          ,0x000056f7000000ece9003ca63978907a00ffd100 -- new signed zone for seaport v1.6
          )
 or  fee_wallet_name = 'opensea'
)

