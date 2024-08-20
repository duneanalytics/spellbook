{{ config(
    schema = 'oneplanet_polygon',
    alias = 'base_trades',

    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'sub_tx_trade_id']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0xf7eB758c21a6d0f9029Da4655B9F24343c3924dB,'oneplanet')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v3_trades(
     blockchain = 'polygon'
     ,source_transactions = source('polygon','transactions')
     ,Seaport_evt_OrderFulfilled = source('oneplanet_polygon','Seaport_evt_OrderFulfilled')
     ,Seaport_call_matchAdvancedOrders = source('oneplanet_polygon','Seaport_call_matchAdvancedOrders')
     ,Seaport_call_matchOrders = source('oneplanet_polygon','Seaport_call_matchOrders')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2023-09-03'
     ,native_currency_contract = '0x0000000000000000000000000000000000001010'
     ,seaport_fork_address = '0xcbbecf690e030d096794f7685a1bf4a58378a575'
     ,project = 'oneplanet'
     ,version = 'v1'
    )
  }}
)

select *
from trades
