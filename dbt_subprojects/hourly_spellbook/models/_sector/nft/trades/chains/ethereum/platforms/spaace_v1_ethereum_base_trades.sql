{{ config(
    schema = 'spaace_v1_ethereum',
    alias = 'base_trades',

    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'sub_tx_trade_id']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values   (0x0a993347f6eeaefa5c101028f793eea9f8c9cb28,'spaace')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v4_trades(
     blockchain = 'ethereum'
     ,source_transactions = source('ethereum','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_ethereum','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('seaport_ethereum','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2025-09-01'
     ,project = 'spaace'
     ,version = 'v1'
    )
  }}
)

select *
from trades
where
( zone_address in (0xcB560823FDb487f68361c0c6CC5Fd9E6b7D54063
          )
 or  fee_wallet_name = 'spaace'
)

