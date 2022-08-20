 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      ('0xb3e808e102ac4be070ee3daac70672ffc7c1adca', 'Element') -- Element NFT Marketplace Aggregator
  ) AS temp_table (contract_address, name)