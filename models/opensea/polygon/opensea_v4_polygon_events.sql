{{ config(
    schema = 'opensea_v4_polygon',
    alias = 'events',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx']
    )
}}
WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values (0x0000a26b00c1f0df003000390027140000faa719,'opensea')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v4_trades(
     blockchain = 'polygon'
     ,source_transactions = source('polygon','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_polygon','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('seaport_polygon','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0x0000000000000000000000000000000000001010'
     ,native_token_symbol = 'MATIC'
     ,start_date = '2023-02-01'
    )
  }}
)

select *
from trades
where (    fee_wallet_name = 'opensea'
           or right_hash = 0x360c6ebe
         )
and tx_hash != 0x42e8f1d5dca4d45678608c58c1f8d0670513787c2acd92a9dc024e5780664121
and tx_hash != 0x25ce8a1559a48da77eb55c1b95ee0d8c7b6b4c4d9eece89ac00640db54dfac63
and tx_hash != 0xeaa57f3622702b9058ed7529f437aed755a26f132934a38884012c9a4e78799c
