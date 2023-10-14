{{ config(schema = 'tokens_zksync', alias = alias('nft_curated'), tags=['static', 'dunesql']) }}

SELECT
  contract_address, name, symbol
FROM
  (VALUES
  (0xD07180c423F9B8CF84012aA28cC174F3c433EE29,	'LIBERTAS OMNIBUS', 'ZKS')

) as temp_table (contract_address, name, symbol)
