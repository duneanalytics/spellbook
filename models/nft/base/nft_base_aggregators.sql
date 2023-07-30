{{config(
    tags = ['dunesql'],
    alias = alias('aggregators')
)}}

SELECT
  contract_address as contract_address,
  name
FROM
  (
    VALUES
      (0xbbbbbbbe843515689f3182b748b5671665541e58, 'bluesweep') -- bluesweep
      , (0x92D932aBBC7885999c4347880Eb069F854982eDD, 'okx') --okx
      , (0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad, 'Uniswap') -- Uniswap's Universal Router 3
      , (0xc2c862322e9c97d6244a3506655da95f05246fd8, 'Reservoir') -- Reservoir v6.0.1
  ) AS temp_table (contract_address, name)
