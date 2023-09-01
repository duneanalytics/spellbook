{{ config(
    schema = 'opensea_v3_arbitrum',
    alias = alias('events'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0x0000a26b00c1f0df003000390027140000faa719,'opensea')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v3_trades(
     blockchain = 'arbitrum'
     ,source_transactions = source('arbitrum','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_arbitrum','Seaport_evt_OrderFulfilled')
     ,Seaport_call_matchAdvancedOrders = source('seaport_arbitrum','Seaport_call_matchAdvancedOrders')
     ,Seaport_call_matchOrders = source('seaport_arbitrum','Seaport_call_matchOrders')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
     ,native_token_symbol = 'ETH'
     ,start_date = '2022-09-06'
    )
  }}
)

select *
from trades
where (    fee_wallet_name = 'opensea'
           or right_hash = 0x360c6ebe
         )
