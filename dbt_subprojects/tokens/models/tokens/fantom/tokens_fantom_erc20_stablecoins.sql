{{ config(
      alias = 'stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["fantom"]\',
                                  "sector",
                                  "tokens_fantom",
                                  \'["Henrystats"]\') }}'
  )
}}

SELECT contract_address, symbol, decimals, name
FROM (VALUES
          (0xc54A1684fD1bef1f077a336E6be4Bd9a3096a6Ca, '2SHARES', 18, '2SHARE'),
          (0x04068DA6C83AFCFA0e13ba15A6696662335D5B75, 'USDC', 6,	'USD Coin'),
          (0x049d68029688eAbF473097a2fC38ef61633A3C7A, 'fUSDT', 6, 'Frapped USDT'), 
          (0x8D11eC38a3EB5E956B052f67Da8Bdc9bef8Abf3E, 'DAI', 18, 'Dai Stablecoin'),
          (0xdc301622e621166BD8E82f2cA0A26c13Ad0BE355, 'FRAX', 18, 'Frax'),
          (0xAd84341756Bf337f5a0164515b1f6F993D194E1f, 'FUSD', 18, 'Fantom Foundation USD'), 
          (0x7a6e4E3CC2ac9924605DCa4bA31d1831c84b44aE, '2OMB', 18, '2omb Token'), 
          (0x846e4D51d7E2043C1a87E0Ab7490B93FB940357b, 'UST',	6,	'UST (Wormhole)'), 
          (0x87a5C9B60A3aaf1064006FE64285018e50e0d020, 'MAGIK', 18, 'Magik'),
          (0x9879aBDea01a879644185341F7aF7d8343556B7a, 'TUSD', 18, 'TrueUSD'),
          (0x1D3918043d22de2D799a4d80f72Efd50Db90B5Af, 'sPDO', 18, 'pDollar Share'), 
          (0x5f0456F728E2D59028b4f5B8Ad8C604100724C6A, 'L3USD', 18, 'L3USD'), 
          (0xb9D62c829fbF7eAff1EbA4E50F3D0480b66c1748, 'PDO', 18, 'pDollar')
     ) AS temp_table (contract_address, symbol, decimals, name)