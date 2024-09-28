{{config(
    
    schema = 'nft_zksync',
    alias = 'aggregators'
)}}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0x7868a55b638ed298370c16f83fa32b26664726ab, 'Element') -- Element Swap 2
  ) AS temp_table (contract_address, name)
