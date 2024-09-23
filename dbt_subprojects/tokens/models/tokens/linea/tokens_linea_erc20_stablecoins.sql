{{ config(
      schema = 'tokens_linea'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["linea"]\',
                                  "sector",
                                  "tokens_linea",
                                  \'["rantum"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('linea', 0xa219439258ca9da29e9cc4ce5596924745e12b93, 'Fiat-backed stablecoin', 'USDT', 6, ''), 
    ('linea', 0x176211869ca2b568f2a7d4ee941e073a821ee1ff, 'Fiat-backed stablecoin', 'USDC', 6, ''), 
    ('linea', 0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5, 'Fiat-backed stablecoin', 'DAI', 18, ''),
    ('linea', 0xeb466342c4d449bc9f53a865d5cb90586f405215, 'Fiat-backed stablecoin', 'axlUSDC', 6, ''),
    ('linea', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Fiat-backed stablecoin', 'USDe', 18, ''),
    ('linea', 0x7d43aabc515c356145049227cee54b608342c0ad, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
    ('linea', 0xc608dfb90a430df79a8a1edbc8be7f1a0eb4e763, 'Fiat-backed stablecoin', 'arUSD', 18, ''),
    ('linea', 0xa88b54e6b76fb97cdb8ecae868f1458e18a953f4, 'Fiat-backed stablecoin', 'DUSD', 18, ''),
    ('linea', 0x894134a25a5fac1c2c26f1d8fbf05111a3cb9487, 'Fiat-backed stablecoin', 'GRAI', 18, ''),
    ('linea', 0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5, 'Fiat-backed stablecoin', 'DAI', 18, '')
     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
