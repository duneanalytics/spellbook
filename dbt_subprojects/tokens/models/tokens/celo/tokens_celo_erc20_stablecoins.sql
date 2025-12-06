{{ config(
      schema = 'tokens_celo'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["celo"]\',
                                  "sector",
                                  "tokens_celo",
                                  \'["tomfutago"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('celo', 0xfecb3f7c54e2caae9dc6ac9060a822d47e053760, 'Fiat-backed stablecoin', 'BRLA', 18, ''),
    ('celo', 0x8a567e2ae79ca692bd748ab832081c45de4041ea, 'Fiat-backed stablecoin', 'cCOP', 18, ''),
    ('celo', 0xfaea5f3404bba20d3cc2f8c4b0a888f55a3c7313, 'Fiat-backed stablecoin', 'cGHS', 18, ''),
    ('celo', 0x456a3d042c0dbd3db53d5489e98dfb038553b0d0, 'Fiat-backed stablecoin', 'cKES', 18, ''),
    ('celo', 0xe2702bd97ee33c88c8f6f92da3b733608aa76f71, 'Fiat-backed stablecoin', 'cNGN', 18, ''),
    ('celo', 0xc92e8fc2947e32f2b574cca9f2f12097a71d5606, 'Fiat-backed stablecoin', 'COPM', 18, ''),
    ('celo', 0xe8537a3d056da446677b9e9d6c5db704eaab4787, 'Fiat-backed stablecoin', 'cREAL', 18, ''),
    ('celo', 0x4c35853a3b4e647fd266f4de678dcc8fec410bf6, 'Fiat-backed stablecoin', 'cZAR', 18, ''),
    ('celo', 0xc16b81af351ba9e64c1a069e3ab18c244a1e3049, 'Crypto-backed stablecoin', 'agEUR', 18, ''),
    ('celo', 0x73f93dcc49cb8a239e2032663e9475dd5ef29a08, 'Fiat-backed stablecoin', 'eXOF', 18, ''),
    ('celo', 0x105d4a9306d2e55a71d2eb95b81553ae1dc20d7b, 'Fiat-backed stablecoin', 'PUSO', 18, ''),
    ('celo', 0x9346f43c1588b6df1d52bdd6bf846064f92d9cba, 'Fiat-backed stablecoin', 'VEUR', 18, ''),
    ('celo', 0x7ae4265ecfc1f31bc0e112dfcfe3d78e01f4bb7f, 'Fiat-backed stablecoin', 'VGBP', 18, '')

     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)

