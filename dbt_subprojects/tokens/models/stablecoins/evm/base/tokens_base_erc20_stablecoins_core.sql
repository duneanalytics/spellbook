{% set chain = 'base' %}

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
-- new stablecoins should be added to tokens_base_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x60a3e35cc302bfa44cb288bc5a4f316fdb1adb42), -- EURC
     (0xb79dd08ea68a908a97220c76d19a6aa9cbde4376), -- USD+
     (0xcc7ff230365bd730ee4b352cc2492cedac49383e), -- hyUSD
     (0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4), -- eUSD
     (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913), -- USDC
     (0x04d5ddf5f3a8939889f11e97f8c4bb48317f1938), -- USDz
     (0x4621b7a9c75199271f773ebd9a499dbd165c3191), -- DOLA
     (0xca72827a3d211cfd8f6b00ac98824872b72cab49), -- cgUSD
     (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca), -- USDbC
     (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2), -- USDT
     (0xeb466342c4d449bc9f53a865d5cb90586f405215), -- axlUSDC
     (0xc930784d6e14e2fc2a1f49be1068dc40f24762d3), -- cNGN
     (0x1b5f7fa46ed0f487f049c42f374ca4827d65a264), -- dEURO
     (0xa61beb4a3d02decb01039e378237032b351125b4), -- agEUR
     (0x1fca74d9ef54a6ac80ffe7d3b14e76c4330fd5d8), -- VCHF
     (0x18bc5bcc660cf2b9ce3cd51a404afe1a0cbd3c22), -- IDRX
     (0x269cae7dc59803e5c596c95756faeebb6030e0af), -- MXNE
     (0x4ed9df25d38795a47f52614126e47f564d37f347), -- VEUR
     (0xaeb4bb7debd1e5e82266f7c3b5cff56b3a7bf411), -- VGBP
     (0x043eb4b75d0805c43d7c834902e335621983cf03), -- CADC
     (0xb755506531786c8ac63b756bab1ac387bacb0c04), -- ZARP
     (0x50c5725949a6f0c72e6c4a641f24049a917db0cb), -- DAI
     (0x6bb7a212910682dcfdbd5bcbb3e28fb4e8da10ee), -- GHO
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34), -- USDe
     (0x820c137fa70c8691f0e44dc420a5e53c168921dc), -- USDS

     (0x526728dbc96689597f85ae4cd716d4f7fccbae9d), -- msUSD
     (0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189), -- oUSDT
     (0x102d758f688a4c1c5a80b116bd945d4455460282), -- USD₮0
     (0x4e65fe4dba92790696d040ac24aa414708f5c0ab), -- aBasUSDC
     (0x0a1a1a107e45b7ced86833863f482bc5f4ed82ef), -- USDai
     (0x35e5db674d8e93a03d814fa0ada70731efe8a4b9), -- USR
     (0xe4b20925d9e9a62f1e492e15a81dc0de62804dd4), -- BtcUSD
     (0x3a46ed8fceb6ef1ada2e4600a522ae7e24d2ed18), -- USSI
     (0x00000000efe302beaa2b3e6e1b18d08d69a9012a), -- AUSD
     (0xe5020a6d073a794b6e7f05678707de47986fb0b6)  -- frxUSD

) as temp_table (contract_address)
