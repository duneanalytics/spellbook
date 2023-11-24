{{ config(
    schema = 'oneplanet_polygon',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0xf7eB758c21a6d0f9029Da4655B9F24343c3924dB,'oneplanet')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v3_fork_trades(
     blockchain = 'polygon'
     ,source_transactions = source('polygon','transactions')
     ,Seaport_evt_OrderFulfilled = source('oneplanet_polygon','Seaport_evt_OrderFulfilled')
     ,Seaport_call_matchAdvancedOrders = source('oneplanet_polygon','Seaport_call_matchAdvancedOrders')
     ,Seaport_call_matchOrders = source('oneplanet_polygon','Seaport_call_matchOrders')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0x0000000000000000000000000000000000001010'
     ,native_token_symbol = 'MATIC'
     ,start_date = '2023-09-03'
     ,seaport_fork_address = '0xcbbecf690e030d096794f7685a1bf4a58378a575'
     ,project = 'oneplanet'
     ,version = 'v1'
    )
  }}
)

select *
from trades
