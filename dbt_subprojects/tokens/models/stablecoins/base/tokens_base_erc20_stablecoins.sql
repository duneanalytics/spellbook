{% set chain = 'base' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

select '{{chain}}' as blockchain, contract_address, backing, symbol, decimals, name
from (values

     (0x60a3e35cc302bfa44cb288bc5a4f316fdb1adb42, 'Fiat-backed stablecoin', 'EURC', 6, ''),
     (0xb79dd08ea68a908a97220c76d19a6aa9cbde4376, 'Crypto-backed stablecoin', 'USD+', 6, ''),
     (0xcc7ff230365bd730ee4b352cc2492cedac49383e, 'Algorithmic stablecoin', 'hyUSD', 18, ''),
     (0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4, 'Crypto-backed stablecoin', 'eUSD', 18, ''),
     (0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'Fiat-backed stablecoin', 'USDC', 6, ''),
     (0x04d5ddf5f3a8939889f11e97f8c4bb48317f1938, 'Fiat-backed stablecoin', 'USDz', 18, ''),
     (0x4621b7a9c75199271f773ebd9a499dbd165c3191, 'Crypto-backed stablecoin', 'DOLA', 18, ''),
     (0xca72827a3d211cfd8f6b00ac98824872b72cab49, 'Fiat-backed stablecoin', 'cgUSD', 6, ''),
     (0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca, 'Stable-backed stablecoin', 'USDbC', 6, ''),
     (0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 'Stable-backed stablecoin', 'USDT', 6, ''),
     (0xeb466342c4d449bc9f53a865d5cb90586f405215, 'Crypto-backed stablecoin', 'axlUSDC', 6, ''),
     (0xc930784d6e14e2fc2a1f49be1068dc40f24762d3, 'Fiat-backed stablecoin', 'cNGN', 18, ''),
     (0x1b5f7fa46ed0f487f049c42f374ca4827d65a264, 'Fiat-backed stablecoin', 'dEURO', 18, ''),
     (0xa61beb4a3d02decb01039e378237032b351125b4, 'Crypto-backed stablecoin', 'agEUR', 18, ''),
     (0x1fca74d9ef54a6ac80ffe7d3b14e76c4330fd5d8, 'Fiat-backed stablecoin', 'VCHF', 18, ''),
     (0x18bc5bcc660cf2b9ce3cd51a404afe1a0cbd3c22, 'Fiat-backed stablecoin', 'IDRX', 18, ''),
     (0x269cae7dc59803e5c596c95756faeebb6030e0af, 'Fiat-backed stablecoin', 'MXNE', 18, ''),
     (0x4ed9df25d38795a47f52614126e47f564d37f347, 'Fiat-backed stablecoin', 'VEUR', 18, ''),
     (0xaeb4bb7debd1e5e82266f7c3b5cff56b3a7bf411, 'Fiat-backed stablecoin', 'VGBP', 18, ''),
     (0x043eb4b75d0805c43d7c834902e335621983cf03, 'Fiat-backed stablecoin', 'CADC', 18, ''),
     (0xb755506531786c8ac63b756bab1ac387bacb0c04, 'Fiat-backed stablecoin', 'ZARP', 18, ''),
     (0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 'Hybrid stablecoin', 'DAI', 18, 'Sky'),
     (0x6bb7a212910682dcfdbd5bcbb3e28fb4e8da10ee, 'Crypto-backed stablecoin', 'GHO', 18, 'Aave'),
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Crypto-backed stablecoin', 'USDe', 18, 'Ethena'),
     (0x820c137fa70c8691f0e44dc420a5e53c168921dc, 'Hybrid stablecoin', 'USDS', 18, 'Sky')

) as temp_table (contract_address, backing, symbol, decimals, name)
