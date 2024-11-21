{{config(
      
      schema = 'nft_avalanche_c',
      alias='aggregators'
  )
}}
SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0x37ad2bd1e4f1c0109133e07955488491233c9372, 'Element') -- Element NFT Marketplace Aggregator
      , (0x917ef4f231cbd0972a10ec3453f40762c488e6fa, 'Element') -- Element Swap 2
  ) AS temp_table (contract_address, name)
