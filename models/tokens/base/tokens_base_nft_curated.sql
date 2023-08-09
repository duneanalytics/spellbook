{{ config(alias = alias('nft_curated'), tags=['static', 'dunesql']) }}

SELECT
  contract_address, name, symbol
FROM
  (VALUES
  (0x1FC10ef15E041C5D3C54042e52EB0C54CB9b710c,	'Base is for Builders', 'BASEBUILDERS')

) as temp_table (contract_address, name, symbol)
