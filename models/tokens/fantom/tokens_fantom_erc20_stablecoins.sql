{{ config(
      alias='stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["fantom"]\',
                                  "sector",
                                  "tokens_fantom",
                                  \'["Henrystats"]\') }}'
  )
}}

SELECT LOWER(contract_address) as contract_address, symbol, decimals, name
FROM (VALUES
          ('0xc54A1684fD1bef1f077a336E6be4Bd9a3096a6Ca', '2SHARES', 18 '2omb Finance Algorithmic Stablecoin'),
          ('0x04068DA6C83AFCFA0e13ba15A6696662335D5B75', 'USDC', 6,	'Circle USD'),
          ('0x049d68029688eAbF473097a2fC38ef61633A3C7A', 'fUSDT', 6, 'Frapped.io USDT'), 
          ('0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E', 'DAI', 18, 'Makerdao DAI'),
          ('0xdc301622e621166BD8E82f2cA0A26c13Ad0BE355', 'FRAX', 18, 'Frax Finance Stablecoin'),
          ('0xAd84341756Bf337f5a0164515b1f6F993D194E1f', 'FUSD', 18, 'Fantom Foundation USD'), 
          ('0x7a6e4E3CC2ac9924605DCa4bA31d1831c84b44aE', '2OMB', 18, '2omb Finance Algorithmic Stablecoin'), 
          ('0x846e4D51d7E2043C1a87E0Ab7490B93FB940357b', 'UST',	6,	'UST (Wormhole)'), 
          ('0x87a5C9B60A3aaf1064006FE64285018e50e0d020', 'MAGIK', 18, 'Magik Finance Algorithm Stablecoin'),
          ('0x9879aBDea01a879644185341F7aF7d8343556B7a', 'TUSD', 18, 'TrueUSD'),



     ) AS temp_table (contract_address, symbol, decimals, name)