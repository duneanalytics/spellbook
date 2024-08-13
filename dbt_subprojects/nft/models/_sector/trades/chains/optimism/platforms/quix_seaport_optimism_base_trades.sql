{{ config(
    schema = 'quix_seaport_optimism',
    alias = 'base_trades',

    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'sub_tx_trade_id']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0xeC1557A67d4980C948cD473075293204F4D280fd,'quix')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v3_trades(
     blockchain = 'optimism'
     ,source_transactions = source('optimism','transactions')
     ,Seaport_evt_OrderFulfilled = source('quixotic_optimism','Seaport_evt_OrderFulfilled')
     ,Seaport_call_matchAdvancedOrders = source('quixotic_optimism','Seaport_call_matchAdvancedOrders')
     ,Seaport_call_matchOrders = source('quixotic_optimism','Seaport_call_matchOrders')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2022-07-29'
     ,native_currency_contract = '0x4200000000000000000000000000000000000006'
     ,seaport_fork_address = '0x998ef16ea4111094eb5ee72fc2c6f4e6e8647666'
     ,project = 'quix'
    )
  }}
)

select *
from trades
