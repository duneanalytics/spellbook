{% set chain = 'linea' %}

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
-- new stablecoins should be added to tokens_linea_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0xa219439258ca9da29e9cc4ce5596924745e12b93, 'USD'), -- USDT
     (0x176211869ca2b568f2a7d4ee941e073a821ee1ff, 'USD'), -- USDC
     (0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5, 'USD'), -- DAI
     (0xeb466342c4d449bc9f53a865d5cb90586f405215, 'USD'), -- axlUSDC
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'USD'), -- USDe
     (0x7d43aabc515c356145049227cee54b608342c0ad, 'USD'), -- BUSD
     (0xba2f9e7ae9f5f03fce7d560f986743659e768bbf, 'USD'), -- eUSD
     (0xc608dfb90a430df79a8a1edbc8be7f1a0eb4e763, 'USD'), -- arUSD
     (0xa88b54e6b76fb97cdb8ecae868f1458e18a953f4, 'USD'), -- DUSD
     (0x894134a25a5fac1c2c26f1d8fbf05111a3cb9487, 'USD'), -- GRAI
     (0xb79dd08ea68a908a97220c76d19a6aa9cbde4376, 'USD'), -- USD+
     (0x1e1f509963a6d33e169d9497b11c7dbfe73b7f13, 'USD'), -- USDT+
     (0x3ff47c5bf409c86533fe1f4907524d304062428d, 'EUR')  -- EURe


) as temp_table (contract_address, currency)
