{{config(
    
    schema = 'nft_mantle',
    alias = 'aggregators'
)}}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0x9f47921d360aee0651a4f1ed2c4892b4923f9e52, 'Element') -- Element Swap 2
  ) AS temp_table (contract_address, name)
