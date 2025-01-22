{{ config(
    schema = 'tokens_goerli',
    alias = 'nft_curated',
    tags=['static']

)}}

SELECT
  contract_address, name, symbol
FROM
  (VALUES
  (0xE45a9E55B565b118b10a9815B9B2dE789831c6f1, 'C', 'Wrapped Cryptopunks')
   ) as temp_table (contract_address, name, symbol)
