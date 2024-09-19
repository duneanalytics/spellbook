{{ config(
      schema = 'tokens_fantom'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["fantom"]\',
                                  "sector",
                                  "tokens_fantom",
                                  \'["Henrystats", "synthquest"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('fantom', 0x5f0456f728e2d59028b4f5b8ad8c604100724c6a, 'Hybrid stablecoin', 'L3USD', 18, 'L3USD'),
    ('fantom', 0x3129662808bec728a27ab6a6b9afd3cbaca8a43c, 'Crypto-backed stablecoin', 'DOLA', 18, ''),
    ('fantom', 0x9879abdea01a879644185341f7af7d8343556b7a, 'Fiat-backed stablecoin', 'TUSD', 18, 'TrueUSD'),
    ('fantom', 0x0def844ed26409c5c46dda124ec28fb064d90d27, 'Hybrid stablecoin', 'CoUSD', 18, ''),
    ('fantom', 0xb9d62c829fbf7eaff1eba4e50f3d0480b66c1748, 'Hybrid stablecoin', 'PDO', 18, 'pDollar'),
    ('fantom', 0x7a6e4e3cc2ac9924605dca4ba31d1831c84b44ae, 'Hybrid stablecoin', '2OMB', 18, '2omb Token'),
    ('fantom', 0x87a5c9b60a3aaf1064006fe64285018e50e0d020, 'Hybrid stablecoin', 'MAGIK', 18, 'Magik'),
    ('fantom', 0x74e23df9110aa9ea0b6ff2faee01e740ca1c642e, 'Crypto-backed stablecoin', 'TOR', 18, ''),
    ('fantom', 0xfb98b335551a418cd0737375a2ea0ded62ea213b, 'Crypto-backed stablecoin', 'miMATIC', 18, ''),
    ('fantom', 0x04068da6c83afcfa0e13ba15a6696662335d5b75, 'Fiat-backed stablecoin', 'USDC', 6, 'USD Coin'),
    ('fantom', 0x82f0b8b456c1a451378467398982d4834b6829c1, 'Crypto-backed stablecoin', 'MIM', 18, ''),
    ('fantom', 0xdc301622e621166bd8e82f2ca0a26c13ad0be355, 'Hybrid stablecoin', 'FRAX', 18, 'Frax'),
    ('fantom', 0xb67fa6defce4042070eb1ae1511dcd6dcc6a532e, 'Crypto-backed stablecoin', 'alUSD', 18, ''),
    ('fantom', 0x846e4d51d7e2043c1a87e0ab7490b93fb940357b, 'Hybrid stablecoin', 'UST', 6, 'UST (Wormhole)'),
    ('fantom', 0xad84341756bf337f5a0164515b1f6f993d194e1f, 'Hybrid stablecoin', 'FUSD', 18, 'Fantom Foundation USD'),
    ('fantom', 0x049d68029688eabf473097a2fc38ef61633a3c7a, 'Fiat-backed stablecoin', 'fUSDT', 6, 'Frapped USDT'),
    ('fantom', 0xe2d27f06f63d98b8e11b38b5b08a75d0c8dd62b9, 'Algorithmic stablecoin', 'UST', 6, ''),
    ('fantom', 0x8d11ec38a3eb5e956b052f67da8bdc9bef8abf3e, 'Crypto-backed stablecoin', 'DAI', 18, 'Dai Stablecoin'),
    ('fantom', 0x6fc9383486c163fa48becdec79d6058f984f62ca, 'Hybrid stablecoin', 'USDB', 18, ''),
    ('fantom', 0xc54a1684fd1bef1f077a336e6be4bd9a3096a6ca, 'Hybrid stablecoin', '2SHARES', 18, '2SHARE'),
    ('fantom', 0x1d3918043d22de2d799a4d80f72efd50db90b5af, 'Hybrid stablecoin', 'sPDO', 18, 'pDollar Share')


     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
