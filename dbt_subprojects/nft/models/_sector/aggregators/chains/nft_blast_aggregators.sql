{{config(
    
    schema = 'nft_blast',
    alias = 'aggregators'
)}}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0xe29799ca0b98ba41343a4ea52fe15ed7d5e05662, 'Element') -- Element Swap 2
  ) AS temp_table (contract_address, name)
