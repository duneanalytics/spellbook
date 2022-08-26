 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      ('0x56085ea9c43dea3c994c304c53b9915bff132d20', 'Element') -- Element NFT Marketplace Aggregator
  ) AS temp_table (contract_address, name)