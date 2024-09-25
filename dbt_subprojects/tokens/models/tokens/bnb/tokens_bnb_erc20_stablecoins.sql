{{ config(
      schema = 'tokens_bnb'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["bnb"]\',
                                  "sector",
                                  "tokens_bnb",
                                  \'["synthquest"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES

    ('bnb', 0x14016e85a25aeb13065688cafb43044c2ef86784, 'Fiat-backed stablecoin', 'TUSD', 18, ''),
    ('bnb', 0x23396cf899ca06c4472205fc903bdb4de249d6fc, 'Algorithmic stablecoin', 'UST', 18, ''),
    ('bnb', 0x0782b6d8c4551b9760e74c0545a9bcd90bdc41e5, 'Crypto-backed stablecoin', 'HAY', 18, ''),
    ('bnb', 0x90c97f71e18723b0cf0dfa30ee176ab653e89f40, 'Hybrid stablecoin', 'FRAX', 18, ''),
    ('bnb', 0x6bf2be9468314281cd28a94c35f967cafd388325, 'Hybrid stablecoin', 'oUSD', 18, ''),
    ('bnb', 0x55d398326f99059ff775485246999027b3197955, 'Fiat-backed stablecoin', 'USDT', 18, ''),
    ('bnb', 0xde7d1ce109236b12809c45b23d22f30dba0ef424, 'Hybrid stablecoin', 'USDS', 18, ''),
    ('bnb', 0xfa4ba88cf97e282c505bea095297786c16070129, 'Fiat-backed stablecoin', 'CUSD', 18, ''),
    ('bnb', 0xc5f0f7b66764f6ec8c8dff7ba683102295e16409, 'Fiat-backed stablecoin', 'FDUSD', 18, ''),
    ('bnb', 0x2952beb1326accbb5243725bd4da2fc937bca087, 'RWA-backed stablecoin', 'wUSDR', 9, ''),
    ('bnb', 0x1d6cbdc6b29c6afbae65444a1f65ba9252b8ca83, 'Crypto-backed stablecoin', 'TOR', 18, ''),
    ('bnb', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'BOB', 18, ''),
    ('bnb', 0x6458df5d764284346c19d88a104fd3d692471499, 'Hybrid stablecoin', 'iUSDS', 18, ''),
    ('bnb', 0x2f29bc0ffaf9bff337b31cbe6cb5fb3bf12e5840, 'Crypto-backed stablecoin', 'DOLA', 18, ''),
    ('bnb', 0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d, 'Fiat-backed stablecoin', 'USDC', 18, ''),
    ('bnb', 0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'Crypto-backed stablecoin', 'MAI', 18, ''),
    ('bnb', 0x4bd17003473389a42daf6a0a729f6fdb328bbbd7, 'Crypto-backed stablecoin', 'VAI', 18, ''),
    ('bnb', 0xf0186490b18cb74619816cfc7feb51cdbe4ae7b9, 'RWA-backed stablecoin', 'zUSD', 18, ''),
    ('bnb', 0xfe19f0b51438fd612f6fd59c1dbb3ea319f433ba, 'Crypto-backed stablecoin', 'MIM', 18, ''),
    ('bnb', 0xe9e7cea3dedca5984780bafc599bd69add087d56, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
    ('bnb', 0xb5102cee1528ce2c760893034a4603663495fd72, 'Crypto-backed stablecoin', 'USX', 18, ''),
    ('bnb', 0xb7f8cd00c5a06c0537e2abff0b58033d02e5e094, 'Crypto-backed stablecoin', 'PAX', 18, ''),
    ('bnb', 0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3, 'Hybrid stablecoin', 'DAI', 18, ''),
    ('bnb', 0xd17479997f34dd9156deef8f95a52d81d265be9c, 'Algorithmic stablecoin', 'USDD', 18, '')



     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
