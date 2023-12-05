{{ config(
    schema = 'quix_seaport_optimism',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0xeC1557A67d4980C948cD473075293204F4D280fd,'quix')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v3_fork_trades(
     blockchain = 'optimism'
     ,source_transactions = source('optimism','transactions')
     ,Seaport_evt_OrderFulfilled = source('quixotic_optimism','Seaport_evt_OrderFulfilled')
     ,Seaport_call_matchAdvancedOrders = source('quixotic_optimism','Seaport_call_matchAdvancedOrders')
     ,Seaport_call_matchOrders = source('quixotic_optimism','Seaport_call_matchOrders')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0x4200000000000000000000000000000000000006'
     ,native_token_symbol = 'ETH'
     ,start_date = '2022-07-29'
     ,seaport_fork_address = '0x998ef16ea4111094eb5ee72fc2c6f4e6e8647666'
     ,project = 'quix'
    )
  }}
)

select *
from trades
