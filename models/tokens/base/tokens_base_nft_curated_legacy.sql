{{ config(alias = alias('nft_curated', legacy_model=True), tags=['static', 'legacy']) }}

SELECT
  LOWER(contract_address) AS contract_address, name, '' as symbol
FROM
  (VALUES
('0x1FC10ef15E041C5D3C54042e52EB0C54CB9b710c',	'Base is for Builders')


) as temp_table (contract_address, name)
