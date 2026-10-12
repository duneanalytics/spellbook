{{ config(
    schema = 'gondi_v1_ethereum',
    alias = 'base_trades',

    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number', 'tx_hash', 'sub_tx_trade_id']
    )
}}

-- GONDI orders settle on Seaport 1.6. The OpenSea and Spaace fee wallets are listed only so their orders can be excluded below.
WITH fee_wallets as (
    select wallet_address, wallet_name from (
    values   (0x4169447a424ec645f8a24dccfd8328f714dd5562,'gondi')
            ,(0x5b3256965e7c3cf26e11fcaf296dfc8807c01073,'opensea')
            ,(0x8de9c5a032463c561423387a9648c5c7bcc5bc90,'opensea')
            ,(0x34ba0f2379bf9b81d09f7259892e26a8b0885095,'opensea')
            ,(0x0000a26b00c1f0df003000390027140000faa719,'opensea')
            ,(0x0a993347f6eeaefa5c101028f793eea9f8c9cb28,'spaace')
    ) as foo(wallet_address, wallet_name)
)
, trades as (
    {{ seaport_v4_trades(
     blockchain = 'ethereum'
     ,source_transactions = source('ethereum','transactions')
     ,Seaport_evt_OrderFulfilled = source('seaport_ethereum','Seaport_evt_OrderFulfilled')
     ,Seaport_evt_OrdersMatched = source('seaport_ethereum','Seaport_evt_OrdersMatched')
     ,fee_wallet_list_cte = 'fee_wallets'
     ,start_date = '2025-11-19'
     ,project = 'gondi'
     ,version = 'v1'
    )
  }}
)

select *
from trades
where project_contract_address = 0x0000000000000068f116a894984e2db1123eb395 -- Seaport 1.6
  -- fee_wallet_name is the max() over a trade's fee legs, so any OpenSea or Spaace leg wins over 'gondi'
  and coalesce(fee_wallet_name, 'gondi') = 'gondi'
  and zone_address not in (0xf397619df7bfd4d1657ea9bdd9df7ff888731a11 -- OpenSea zones, as in opensea_v4_ethereum_base_trades
          ,0x9b814233894cd227f561b78cc65891aa55c62ad2
          ,0x004c00500000ad104d7dbd00e3ae0a5c00560c00
          ,0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd
          ,0x000000e7ec00e7b300774b00001314b8610022b8
          ,0x000056f7000000ece9003ca63978907a00ffd100
          ,0x0000000000d80cfcb8dfcd8b2c4fd9c813482938 -- Blur, as in blur_seaport_ethereum_base_trades
          ,0xcb560823fdb487f68361c0c6cc5fd9e6b7d54063 -- Spaace, as in spaace_v1_ethereum_base_trades
          )
  and ( fee_wallet_name = 'gondi'
     or zone_address = 0xad491cbcf997e701436c23f32857fef454bde01f -- GONDI signed zone
     or right_hash = 0xe8feb17d -- first 4 bytes of keccak256('gondi.xyz'), appended to Seaport calldata by gondi.xyz
     or tx_to in (0x3b59bffe109e0f33f20887343759a98b48ecdf5f -- GONDI PurchaseBundler (buy or sell with a loan)
          ,0xcea7eea12c6fc82d0318704b9d35a4192c2d260a
          ,0xf46a58cada29ff34cf62f72357d2b37815506feb
          ,0x2b5e66c44b223b9d3b192e697f58795febcd6c10
          ,0x53ceda4c47585df08201955820e23bb261489140
          ,0x500007b5ccc7dc461dfc0f0ac99295b2e077c333
          ,0x1fba531724ea2493a15bf5c4ea05f6ab5c0fcd62
          ,0xfd31a0cd628f0bab2cc174c3abd6bfc2d01aca61
          ,0xc10472ac1bf9f2e58ff2c83596b4535334c90814
          ,0xe02d85e80c1ebda5aa43b8a1996332a83f855a59
          )
  )
