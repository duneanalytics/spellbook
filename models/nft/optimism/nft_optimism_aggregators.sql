{{ config( alias='aggregators') }}

SELECT
  lower(contract_address) as contract_address,
  name
FROM
  (
    VALUES
      ('0xbbbbbbbe843515689f3182b748b5671665541e58', 'bluesweep') -- bluesweep
  ) AS temp_table (contract_address, name)
