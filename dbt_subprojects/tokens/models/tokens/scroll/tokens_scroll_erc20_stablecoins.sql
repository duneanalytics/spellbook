{{ config(
      schema = 'tokens_scroll'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["scroll"]\',
                                  "sector",
                                  "tokens_scroll",
                                  \'["rantum"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('scroll', 0xf55bec9cafdbe8730f096aa55dad6d22d44099df, 'Fiat-backed stablecoin', 'USDT', 6, ''),
    ('scroll', 0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4, 'Fiat-backed stablecoin', 'USDC', 6, ''),
    ('scroll', 0xca77eb3fefe3725dc33bccb54edefc3d9f764f97, 'Hybrid stablecoin', 'DAI', 18, ''),
    ('scroll', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Crypto-backed stablecoin', 'USDe', 18, ''),
    ('scroll', 0x77fbf86399ed764a084f77b9accb049f3dbc32d2, 'Crypto-backed stablecoin', 'loreUSD', 18, ''),
    ('scroll', 0xedeabc3a1e7d21fe835ffa6f83a710c70bb1a051, 'Crypto-backed stablecoin', 'LUSD', 18, '')
     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
