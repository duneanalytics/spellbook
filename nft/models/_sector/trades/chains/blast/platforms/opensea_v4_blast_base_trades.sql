{{ config(
    schema = 'opensea_v4_blast',
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
    {{ seaport_v4_trades(
     blockchain = 'blast'
     ,source_transactions = source('blast','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_blast','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('seaport_blast','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2024-03-21'
     ,native_currency_contract = '0x4200000000000000000000000000000000000006'
    )
  }}
)

select *
from trades
where (    fee_wallet_name = 'opensea'
           or right_hash = 0x360c6ebe
         )