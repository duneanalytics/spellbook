{{config(
      tags = ['dunesql'],
      schema = 'nft_avalanche_c',
      alias=alias('aggregators')
  )
}}
SELECT
  contract_address,
  name
FROM
  (
    VALUES
      (0x37ad2bd1e4f1c0109133e07955488491233c9372, 'Element') -- Element NFT Marketplace Aggregator
  ) AS temp_table (contract_address, name)
