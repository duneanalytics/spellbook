{{ config(
    schema = 'opensea_v3_optimism',
    alias = 'base_trades',

    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'sub_tx_trade_id']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0x0000a26b00c1f0df003000390027140000faa719,'opensea')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
     {{ seaport_v3_trades(
     blockchain = 'optimism'
     ,source_transactions = source('optimism','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_optimism','Seaport_evt_OrderFulfilled')
     ,Seaport_call_matchAdvancedOrders = source('seaport_optimism','Seaport_call_matchAdvancedOrders')
     ,Seaport_call_matchOrders = source('seaport_optimism','Seaport_call_matchOrders')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2022-07-01'
     ,native_currency_contract = '0x4200000000000000000000000000000000000006'
    )
  }}
)

select *
from trades
where (    fee_wallet_name = 'opensea'
           or right_hash = 0x360c6ebe
         )
