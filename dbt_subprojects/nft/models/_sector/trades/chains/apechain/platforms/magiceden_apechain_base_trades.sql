{{ config(
    schema = 'magiceden_apechain',
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
     blockchain = 'apechain'
     ,source_transactions = source('apechain','transactions')
     ,Seaport_evt_OrderFulfilled = source('opensea_apechain','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('opensea_apechain','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2024-09-02'
     ,native_currency_contract = '0x48b62137edfa95a428d35c09e44256a739f6b557'
     ,project = 'magiceden'
     ,version = 'v1'
    )
  }}
)

select *
from trades
where (    fee_wallet_name = 'magiceden'
          --  or right_hash = 0x360c6ebe
         )