{% set chain = 'optimism' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_optimism_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xfb21b70922b9f6e3c6274bcd6cb1aa8a0fe20b80), -- UST
     (0x970d50d09f3a656b43e11b0d45241a84e3a6e011), -- DAI+
     (0xdfa46478f9e5ea86d57387849598dbfb2e964b02), -- MAI
     (0x3666f603cc164936c1b87e207f36beba4ac5f18a), -- hUSD
     (0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9), -- sUSD
     (0xda10009cbd5d07dd0cecc66161fc93d7c9000da1), -- DAI
     (0xc40f949f8a4e094d1b49a23ea9241d289b7b2819), -- LUSD
     (0x25d8039bb044dc227f741a9e381ca4ceae2e6ae8), -- hUSDC
     (0x79af5dd14e855823fa3e9ecacdf001d99647d043), -- EUR
     (0xfbc4198702e81ae77c06d58f81b629bdf36f0a71), -- sEUR
     (0xcb59a0a753fdb7491d5f3d794316f1ade197b21e), -- TUSD
     (0x7fb688ccf682d58f86d7e38e03f9d22e7705448b), -- RAI
     (0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a), -- alUSD
     (0x340fe1d898eccaad394e2ba0fc1f93d27c7b717a), -- wUSDR
     (0x2e3d870790dc77a83dd1d18184acc7439a53f475), -- FRAX
     (0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b), -- BOB
     (0x8ae125e8653821e851f12a49f7765db9a9ce7384), -- DOLA
     (0x0b2c639c533813f4aa9d7837caf62653d097ff85), -- USDC
     (0x67c10c397dd0ba417329543c1a40eb48aaa7cd00), -- nUSD
     (0x56900d66d74cb14e3c86895789901c9135c95b16), -- hDAI
     (0x73cb180bf0521828d8849bc8cf2b920918e23032), -- USD+
     (0x59d9356e565ab3a36dd77763fc0d87feaf85508c), -- USDM
     (0xb153fb3d196a8eb25522705560ac152eeec57901), -- MIM
     (0x94b008aa00579c1307b0ef2c499ad98a8ce58e58), -- USDT
     (0x7f5c764cbc14f9669b88837ca1490cca17c31607), -- USDC.e
     (0xa3a538ea5d5838dc32dde15946ccd74bdd5652ff), -- sINR
     (0x9c9e5fd8bbc25984b178fdce6117defa39d2db39), -- BUSD
     (0x7113370218f31764c1b6353bdf6004d86ff6b9cc), -- USDD
     (0x2057c8ecb70afd7bee667d76b4cd373a325b1a20), -- hUSDT
     (0x9485aca5bbbe1667ad97c7fe7c4531a624c8b1ed), -- agEUR
     (0xbfd291da8a403daaf7e5e9dc1ec0aceacd4848b9), -- USX
     (0xba28feb4b6a6b81e3f26f08b83a19e715c4294fd), -- UST
     (0xeb466342c4d449bc9f53a865d5cb90586f405215), -- axlUSDC
     (0x4bdc50829003cc017443bf9bfb3ac82f3f0c4ad4), -- CLPC
     (0x1b5f7fa46ed0f487f049c42f374ca4827d65a264), -- dEURO
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34), -- USDe
     (0x4f13a96ec5c4cf34e442b46bbd98a0791f20edc3), -- USDS

     (0x01bff41798a0bcf287b996046ca68b395dbc1071), -- USD₮0
     (0x625e7708f30ca75bfd92586e17077590c60eb4cd), -- aOptUSDC
     (0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189), -- oUSDT
     (0x9dabae7274d28a45f0b65bf8ed201a5731492ca0)  -- msUSD

) as temp_table (contract_address)
