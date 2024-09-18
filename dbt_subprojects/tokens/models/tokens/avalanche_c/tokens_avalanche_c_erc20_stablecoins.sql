{{ config(
      schema = 'tokens_avalanche_c'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                  "sector",
                                  "tokens_avalanche_c",
                                  \'["synthquest"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES

        ('avalanche_c', 0x111111111111ed1d73f860f57b2798b683f2d325, 'Crypto-backed stablecoin', 'YUSD', 18, ''),
        ('avalanche_c', 0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7, 'Fiat-backed stablecoin', 'USDt', 6, ''),
        ('avalanche_c', 0xab05b04743e0aeaf9d2ca81e5d3b8385e4bf961e, 'Hybrid stablecoin', 'USDS', 18, ''),
        ('avalanche_c', 0x00000000efe302beaa2b3e6e1b18d08d69a9012a, 'Fiat-backed stablecoin', 'AUSD', 6, ''),
        ('avalanche_c', 0x130966628846bfd36ff31a822705796e8cb8c18d, 'Crypto-backed stablecoin', 'MIM', 18, ''),
        ('avalanche_c', 0xd24c2ad096400b6fbcd2ad8b24e7acbc21a1da64, 'Hybrid stablecoin', 'FRAX', 18, ''),
        ('avalanche_c', 0xd586e7f844cea2f87f50152665bcbc2c279d8d70, 'Crypto-backed stablecoin', 'DAI.e', 18, ''),
        ('avalanche_c', 0x3b55e45fd6bd7d4724f5c47e0d1bcaedd059263e, 'Crypto-backed stablecoin', 'miMatic', 18, ''),
        ('avalanche_c', 0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e, 'Fiat-backed stablecoin', 'USDC', 6, ''),
        ('avalanche_c', 0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
        ('avalanche_c', 0xf14f4ce569cb3679e99d5059909e23b07bd2f387, 'Crypto-backed stablecoin', 'NXUSD', 18, ''),
        ('avalanche_c', 0x1c20e891bab6b1727d14da358fae2984ed9b59eb, 'Fiat-backed stablecoin', 'TUSD', 18, ''),
        ('avalanche_c', 0xdacde03d7ab4d81feddc3a20faa89abac9072ce2, 'Crypto-backed stablecoin', 'USP', 18, ''),
        ('avalanche_c', 0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664, 'Crypto-backed stablecoin', 'USDC.e', 6, ''),
        ('avalanche_c', 0x8861f5c40a0961579689fdf6cdea2be494f9b25a, 'Hybrid stablecoin', 'iUSDS', 18, '')

     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
