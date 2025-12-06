{{ config(
      schema = 'tokens_worldchain'
      , alias = 'erc20_stablecoins'
      , tags=['static']
      , post_hook='{{ expose_spells(\'["worldchain"]\',
                                  "sector",
                                  "tokens_worldchain",
                                  \'["tomfutago"]\') }}'
      , unique_key = ['contract_address']
  )
}}

SELECT blockchain, contract_address, backing, symbol, decimals, name
FROM (VALUES
    ('worldchain', 0x18bc5bcc660cf2b9ce3cd51a404afe1a0cbd3c22, 'Fiat-backed stablecoin', 'IDRX', 18, '')

     ) AS temp_table (blockchain, contract_address, backing, symbol, decimals, name)

