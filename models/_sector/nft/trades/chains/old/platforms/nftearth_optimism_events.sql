{{ config(
    schema = 'nftearth_optimism',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0xd55c6b0a208362b18beb178e1785cf91c4ce937a,'nftearth')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v3_fork_trades(
     blockchain = 'optimism'
     ,source_transactions = source('optimism','transactions')
     ,Seaport_evt_OrderFulfilled = source('nftearth_optimism','Seaport_evt_OrderFulfilled')
     ,Seaport_call_matchAdvancedOrders = source('nftearth_optimism','Seaport_call_matchAdvancedOrders')
     ,Seaport_call_matchOrders = source('nftearth_optimism','Seaport_call_matchOrders')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0x4200000000000000000000000000000000000006'
     ,native_token_symbol = 'ETH'
     ,start_date = '2023-01-31'
     ,seaport_fork_address = '0x0f9b80fc3c8b9123d0aef43df58ebdbc034a8901'
     ,project = 'nftearth'
     ,version = 'v1'
    )
  }}
)

select *
from trades
