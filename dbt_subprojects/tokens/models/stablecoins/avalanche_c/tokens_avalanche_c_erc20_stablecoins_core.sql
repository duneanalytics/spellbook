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

select '{{chain}}' as blockchain, contract_address
from (values

     (0x111111111111ed1d73f860f57b2798b683f2d325), -- YUSD
     (0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7), -- USDt
     (0xab05b04743e0aeaf9d2ca81e5d3b8385e4bf961e), -- USDS
     (0x00000000efe302beaa2b3e6e1b18d08d69a9012a), -- AUSD
     (0x130966628846bfd36ff31a822705796e8cb8c18d), -- MIM
     (0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64), -- FRAX
     (0xd586e7f844cea2f87f50152665bcbc2c279d8d70), -- DAI.e
     (0x3b55e45fd6bd7d4724f5c47e0d1bcaedd059263e), -- miMatic
     (0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e), -- USDC
     (0x9c9e5fd8bbc25984b178fdce6117defa39d2db39), -- BUSD
     (0xf14f4ce569cb3679e99d5059909e23b07bd2f387), -- NXUSD
     (0x1c20e891bab6b1727d14da358fae2984ed9b59eb), -- TUSD
     (0xdacde03d7ab4d81feddc3a20faa89abac9072ce2), -- USP
     (0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664), -- USDC.e
     (0x8861f5c40a0961579689fdf6cdea2be494f9b25a), -- iUSDS
     (0xc7198437980c041c805a1edcba50c1ce5db95118), -- USDT.e
     (0xabe7a9dfda35230ff60d1590a929ae0644c47dc1), -- aUSD
     (0xfab550568c688d5d8a52c7d794cb93edc26ec0ec), -- axlUSD
     (0x491a4eb4f1fc3bff8e1d2fc856a6a46663ad556f), -- BRZ
     (0x8835a2f66a7aaccb297cb985831a616b75e2e16c), -- EUROP
     (0xc891eb4cbdeff6e073e859e987815ed1505c2acd), -- EURC
     (0x228a48df6819ccc2eca01e2192ebafffdad56c19), -- VCHF
     (0xb2f85b7ab3c2b6f62df06de6ae7d09c010a5096e), -- XSGD
     (0xf197ffc28c23e0309b5559e7a166f2c6164c80aa), -- MXNB
     (0x7678e162f38ec9ef2bfd1d0aaf9fd93355e5fa0b), -- VEUR
     (0xe7c3d8c9a439fede00d2600032d5db0be71c3c29), -- JPYC
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34), -- USDe

     (0x24de8771bc5ddb3362db529fc3358f2df3a0e346)  -- avUSD

) as temp_table (contract_address)
