 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      ('0x56085ea9c43dea3c994c304c53b9915bff132d20', 'Element') -- Element NFT Marketplace Aggregator
      , ('0x48939e2b2549710df8b7d9085207279a8f0fe3e5', 'Oxalus') -- Oxalus NFT Aggregator
  ) AS temp_table (contract_address, name)