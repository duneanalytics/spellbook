{{ config(
      schema = 'tokens_gnosis'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["gnosis"]\',
                                  "sector",
                                  "tokens_gnosis",
                                  \'["synthquest","tomfutago"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('gnosis', 0xdd96b45877d0e8361a4ddb732da741e97f3191ff, 'Fiat-backed stablecoin', 'BUSD', 18, ''),
    ('gnosis', 0x4ecaba5870353805a9f068101a40e0f32ed605c6, 'Fiat-backed stablecoin', 'USDT', 6, ''),
    ('gnosis', 0x3f56e0c36d275367b8c502090edf38289b3dea0d, 'Crypto-backed stablecoin', 'MAI', 18, ''),
    ('gnosis', 0x44fa8e6f47987339850636f88629646662444217, 'Hybrid stablecoin', 'DAI', 18, ''),
    ('gnosis', 0xddafbb505ad214d7b80b1f830fccc89b60fb7a83, 'Fiat-backed stablecoin', 'USDC', 6, ''),
    ('gnosis', 0xfecb3f7c54e2caae9dc6ac9060a822d47e053760, 'Fiat-backed stablecoin', 'BRLA', 18, ''),
    ('gnosis', 0x4b1e2c2762667331bc91648052f646d1b0d35984, 'Crypto-backed stablecoin', 'agEUR', 18, ''),
    ('gnosis', 0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430, 'Fiat-backed stablecoin', 'EURe', 18, '')

     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)
