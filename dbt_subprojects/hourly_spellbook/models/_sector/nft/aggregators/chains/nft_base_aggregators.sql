{{config(
    
    schema = 'nft_base',
    alias = 'aggregators'
)}}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0x66950320086664429c69735318724ae24ec0d835, 'Element') -- Element Swap 2
  ) AS temp_table (contract_address, name)
