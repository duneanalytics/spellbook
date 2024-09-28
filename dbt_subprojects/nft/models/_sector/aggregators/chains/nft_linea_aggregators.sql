{{config(
    
    schema = 'nft_linea',
    alias = 'aggregators'
)}}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0x42c759a719c228050901299b88fd316c3a050617, 'Element') -- Element Swap 2
  ) AS temp_table (contract_address, name)
