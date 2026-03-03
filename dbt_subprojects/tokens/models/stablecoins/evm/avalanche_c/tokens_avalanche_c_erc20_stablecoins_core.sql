{% set chain = 'avalanche_c' %}

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
-- new stablecoins should be added to tokens_avalanche_c_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x111111111111ed1d73f860f57b2798b683f2d325, 'USD'), -- YUSD
     (0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7, 'USD'), -- USDt
     (0xab05b04743e0aeaf9d2ca81e5d3b8385e4bf961e, 'USD'), -- USDS
     (0x00000000efe302beaa2b3e6e1b18d08d69a9012a, 'USD'), -- AUSD
     (0x130966628846bfd36ff31a822705796e8cb8c18d, 'USD'), -- MIM
     (0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64, 'USD'), -- FRAX
     (0xd586e7f844cea2f87f50152665bcbc2c279d8d70, 'USD'), -- DAI.e
     (0x3b55e45fd6bd7d4724f5c47e0d1bcaedd059263e, 'USD'), -- miMatic
     (0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e, 'USD'), -- USDC
     (0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'USD'), -- BUSD
     (0xf14f4ce569cb3679e99d5059909e23b07bd2f387, 'USD'), -- NXUSD
     (0x1c20e891bab6b1727d14da358fae2984ed9b59eb, 'USD'), -- TUSD
     (0xdacde03d7ab4d81feddc3a20faa89abac9072ce2, 'USD'), -- USP
     (0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664, 'USD'), -- USDC.e
     (0xc7198437980c041c805a1edcba50c1ce5db95118, 'USD'), -- USDT.e
     (0xfab550568c688d5d8a52c7d794cb93edc26ec0ec, 'USD'), -- axlUSD
     (0x491a4eb4f1fc3bff8e1d2fc856a6a46663ad556f, 'BRL'), -- BRZ
     (0x8835a2f66a7aaccb297cb985831a616b75e2e16c, 'EUR'), -- EUROP
     (0xc891eb4cbdeff6e073e859e987815ed1505c2acd, 'EUR'), -- EURC
     (0x228a48df6819ccc2eca01e2192ebafffdad56c19, 'CHF'), -- VCHF
     (0xb2f85b7ab3c2b6f62df06de6ae7d09c010a5096e, 'SGD'), -- XSGD
     (0xf197ffc28c23e0309b5559e7a166f2c6164c80aa, 'MXN'), -- MXNB
     (0x7678e162f38ec9ef2bfd1d0aaf9fd93355e5fa0b, 'EUR'), -- VEUR
     (0xe7c3d8c9a439fede00d2600032d5db0be71c3c29, 'JPY'), -- JPYC
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'USD'), -- USDe

     (0x24de8771bc5ddb3362db529fc3358f2df3a0e346, 'USD')  -- avUSD

) as temp_table (contract_address, currency)
