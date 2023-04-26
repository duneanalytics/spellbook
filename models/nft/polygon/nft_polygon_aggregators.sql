 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      ('0xb3e808e102ac4be070ee3daac70672ffc7c1adca', 'Element') -- Element NFT Marketplace Aggregator
     ,('0x5e06c349a4a1b8dde8da31e0f167d1cb1d99967c', 'Dew') -- Dew
  ) AS temp_table (contract_address, name)