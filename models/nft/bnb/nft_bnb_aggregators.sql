 {{ config( alias='aggregators') }}

SELECT
  contract_address,
  name
FROM
  (
    VALUES
      ('0x56085ea9c43dea3c994c304c53b9915bff132d20', 'Element') -- Element NFT Marketplace Aggregator
      , ('0x48939e2b2549710df8b7d9085207279a8f0fe3e5', 'Oxalus') -- Oxalus NFT Aggregator
      , ('0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad', 'Uniswap') -- Uniswap's Universal Router
      , ('0xc2c862322e9c97d6244a3506655da95f05246fd8', 'Reservoir') -- Reservoir v6.0.1
  ) AS temp_table (contract_address, name)