{{ config(
      schema = 'tokens_polygon'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["polygon"]\',
                                  "sector",
                                  "tokens_polygon",
                                  \'["synthquest"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('polygon', 0x2791bca1f2de4661ed88a30c99a7a9449aa84174, 'Fiat-backed stablecoin', 'USDC', 6, ''),
    ('polygon', 0x3c499c542cef5e3811e1192ce70d8cc03d5c3359, 'Fiat-backed stablecoin', 'USDC', 6, ''),
    ('polygon', 0x692597b009d13c4049a947cab2239b7d6517875f, 'Algorithmic stablecoin', 'UST', 18, ''),
    ('polygon', 0xcf66eb3d546f0415b368d98a95eaf56ded7aa752, 'Crypto-backed stablecoin', 'USX', 18, ''),
    ('polygon', 0x8f3cf7ad23cd3cadbd9735aff958023239c6a063, 'Hybrid stablecoin', 'DAI', 18, ''),
    ('polygon', 0x2e1ad108ff1d8c782fcbbb89aad783ac49586756, 'Fiat-backed stablecoin', 'TUSD', 18, ''),
    ('polygon', 0xd86b5923f3ad7b585ed81b448170ae026c65ae9a, 'Hybrid stablecoin', 'IRON', 18, ''),
    ('polygon', 0xffa4d863c96e743a2e1513824ea006b8d0353c57, 'Algorithmic stablecoin', 'USDD', 18, ''),
    ('polygon', 0x9c9e5fd8bbc25984b178fdce6117defa39d2db39, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
    ('polygon', 0x49a0400587a7f65072c87c4910449fdcc5c47242, 'Crypto-backed stablecoin', 'MIM', 18, ''),
    ('polygon', 0x2f1b1662a895c6ba01a99dcaf56778e7d77e5609, 'Hybrid stablecoin', 'USDS', 18, ''),
    ('polygon', 0xaf0d9d65fc54de245cda37af3d18cbec860a4d4b, 'RWA-backed stablecoin', 'wUSDR', 9, ''),
    ('polygon', 0x45c32fa6df82ead1e2ef74d17b76547eddfaff89, 'Hybrid stablecoin', 'FRAX', 18, ''),
    ('polygon', 0x66f31345cb9477b427a1036d43f923a557c432a4, 'Hybrid stablecoin', 'iUSDS', 18, ''),
    ('polygon', 0xb0b195aefa3650a6908f15cdac7d92f8a5791b0b, 'Crypto-backed stablecoin', 'BOB', 18, ''),
    ('polygon', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f, 'Fiat-backed stablecoin', 'USDT', 6, ''),
    ('polygon', 0xa3fa99a148fa48d14ed51d610c367c61876997f1, 'Crypto-backed stablecoin', 'miMATIC', 18, ''),
    ('polygon', 0xdab529f40e671a1d4bf91361c21bf9f0c9712ab7, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
    ('polygon', 0x3a3e7650f8b9f667da98f236010fbf44ee4b2975, 'Crypto-backed stablecoin', 'xUSD', 18, ''),
    ('polygon', 0x23001f892c0c82b79303edc9b9033cd190bb21c7, 'Crypto-backed stablecoin', 'LUSD', 18, '')


     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
