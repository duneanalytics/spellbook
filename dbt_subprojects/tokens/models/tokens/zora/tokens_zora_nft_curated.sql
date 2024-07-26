{{ config(
  schema = 'tokens_zora'
  ,alias = 'nft_curated'
  , tags=['static']) }}

SELECT
  contract_address, name, symbol
FROM
  (VALUES
   (0x1F781d47cD59257D7AA1Bd7b2fbaB50D57AF8587,	'BLOCKS', 'BLOCKS')
  ,(0x1225c2d6e987aa95b76e0d03b505696a1b7a080f, 'Serene Lake Twilight', 'SLT')
  ,(0xbc2ca61440faf65a9868295efa5d5d87c55b9529, 'sqr(16)', 'SQR')
  ,(0x53cb0b849491590cab2cc44af8c20e68e21fc36d, 'Allure', 'ALL')
  ,(0x4073a52a3fc328d489534ab908347ec1fcb18f7f, 'GoldenFla', 'GFLA')
  ,(0x8974b96da5886ed636962f66a6456dc39118a140, 'Zoggles', '.ZOGGLE...')
  ,(0x199a21f0be1cdcdd882865e7d0f462e4778c5ee4, 'Galaxy Zorb', 'GZRB')
  ,(0x9eae90902a68584e93a83d7638d3a95ac67fc446, 'Fla', 'FLA')

) as temp_table (contract_address, name, symbol)
