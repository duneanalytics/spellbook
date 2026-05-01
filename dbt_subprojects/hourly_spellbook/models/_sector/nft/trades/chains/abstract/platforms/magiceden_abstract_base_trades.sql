{{ config(
    schema = 'magiceden_abstract',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0x6fa303e72bed54f515a513496f922bc331e2f27e,'magiceden')
    ) as foo(wallet_address, wallet_name)
)

, trades as (
    {{ seaport_v4_trades(
     blockchain = 'abstract'
     ,source_transactions = source('abstract','transactions')
     ,Seaport_evt_OrderFulfilled = source('opensea_abstract','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('opensea_abstract','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2024-11-12'
     ,native_currency_contract = '0x0000000000000000000000000000000000000000'
     ,Seaport_order_contracts = [
        '0xdf3969a315e3fc15b89a2752d0915cc76a5bd82d'
     ]
     ,project = 'magiceden'
     ,version = 'v1'
    )
  }}
)

select *
from trades
where (    fee_wallet_name = 'magiceden'
           or right_hash = 0x360c6ebe
         )