{{ config(
    schema = 'opensea_v4_ethereum',
    alias = 'events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index', 'nft_contract_address', 'token_id', 'sub_type', 'sub_idx', 'number_of_items']
    )
}}

WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values   (0x5b3256965e7c3cf26e11fcaf296dfc8807c01073,'opensea')
            ,(0x8de9c5a032463c561423387a9648c5c7bcc5bc90,'opensea')
            ,(0x34ba0f2379bf9b81d09f7259892e26a8b0885095,'opensea')
            ,(0x0000a26b00c1f0df003000390027140000faa719,'opensea')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v4_trades(
     blockchain = 'ethereum'
     ,source_transactions = source('ethereum','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_ethereum','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('seaport_ethereum','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,native_token_address = '0x0000000000000000000000000000000000000000'
     ,alternative_token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
     ,native_token_symbol = 'ETH'
     ,start_date = '2023-02-01'
    )
  }}
)

select *
from trades
where
( zone_address in (0xf397619df7bfd4d1657ea9bdd9df7ff888731a11
          ,0x9b814233894cd227f561b78cc65891aa55c62ad2
          ,0x004c00500000ad104d7dbd00e3ae0a5c00560c00
          ,0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd
          ,0x000000e7ec00e7b300774b00001314b8610022b8 -- newly added on seaport v1.4
          ,0x000056f7000000ece9003ca63978907a00ffd100 -- new signed zone for seaport v1.6
          )
 or  fee_wallet_name = 'opensea'
)
-- temporary fix to exclude duplicates
and tx_hash not in (
select tx_hash from
trades
group by tx_hash, evt_index, nft_contract_address, token_id, sub_type, sub_idx
having count(*) > 1
)
