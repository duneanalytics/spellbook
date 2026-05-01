{{config(
    
    schema = 'nft_scroll',
    alias = 'aggregators'
)}}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0x217efe077801387d125fe98e1b61cdda4d1364d2, 'Element') -- Element Swap 2
  ) AS temp_table (contract_address, name)
