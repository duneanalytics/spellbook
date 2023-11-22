{{ config(
  schema = 'tokens_zora'
  ,alias = 'nft_curated'
  , tags=['static']) }}

SELECT
  contract_address, name, symbol
FROM
  (VALUES
  (0x1F781d47cD59257D7AA1Bd7b2fbaB50D57AF8587,	'BLOCKS', 'BLOCKS')

) as temp_table (contract_address, name, symbol)
