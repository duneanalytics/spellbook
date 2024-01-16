{{ config(
    schema = 'mooar_polygon',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}
WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0xdd55acb1855cfec31d0f08c80da1db9862a50660,'mooar')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v4_fork_trades(
     blockchain = 'polygon'
     ,source_transactions = source('polygon','transactions')
     ,Seaport_evt_OrderFulfilled = source('gashero_polygon','MOOAR_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('gashero_polygon','MOOAR_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0x0000000000000000000000000000000000001010'
     ,native_token_symbol = 'MATIC'
     ,start_date = '2023-08-18'
     ,Seaport_order_contracts = ['0xaaaaaaaa33d3520a2266ce508bc079fcfe82c8e3']
     ,project = 'mooar'
     ,version = 'v1'
    )
  }}
)

select *
from trades
