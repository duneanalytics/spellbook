{{ config(
      schema = 'tokens_blast'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["blast"]\',
                                  "sector",
                                  "tokens_blast",
                                  \'["rantum"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('blast', 0x4300000000000000000000000000000000000003, 'Hybrid stablecoin', 'USDB', 18, ''),
    ('blast', 0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34, 'Crypto-backed stablecoin', 'USDe', 18, ''),
    ('blast', 0xeb466342c4d449bc9f53a865d5cb90586f405215, 'Crypto-backed stablecoin', 'axlUSDC', 6, ''),
    ('blast', 0x211cc4dd073734da055fbf44a2b4667d5e5fe5d2, 'Crypto-backed stablecoin', 'sUSDe', 18, ''),
    ('blast', 0x52056ed29fe015f4ba2e3b079d10c0b87f46e8c6, 'Fiat-backed stablecoin', 'USDz', 18, ''),
    ('blast', 0x9d889e2d7ff49ece580e5354ce934ccd1d6e78dc, 'Crypto-backed stablecoin', 'expUSD', 18, ''),
    ('blast', 0x1a3d9b2fa5c6522c8c071dc07125ce55df90b253, 'Fiat-backed stablecoin', 'DUSD', 18, ''),
    ('blast', 0xc608dfb90a430df79a8a1edbc8be7f1a0eb4e763, 'Crypto-backed stablecoin', 'arUSD', 18, ''),
    ('blast', 0x836aed3b0e0ee44c77e0b6db34d170abcce9baac, 'Fiat-backed stablecoin', 'USDBx', 18, ''),
    ('blast', 0x578122317baca7a3c7bb5301460d2f4f96e9394a, 'Crypto-backed stablecoinn', 'DUSD', 18, ''),
    ('blast', 0x837fe561e9c5dfa73f607fda679295dbc2be5e40, 'Crypto-backed stablecoin', 'MUSD', 18, '')
    
    
     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
