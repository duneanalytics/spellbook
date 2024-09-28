{{ config(
     schema = 'op_token_optimism'
        , alias = 'metadata'
        , unique_key = ['contract_address']
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_token",
                                  \'["msilb7"]\') }}'
  )
}}

-- https://community.optimism.io/docs/governance/allocations/#

WITH global_values AS (

  SELECT
    'optimism' AS native_blockchain,
    contract_address,
    symbol,
    decimals,
    cast(POWER(2,32) as double) AS total_initial_supply,
    cast('2022-05-31' AS date) AS token_launch_date

    FROM {{source('tokens_optimism', 'erc20')}} t
    WHERE t.contract_address = 0x4200000000000000000000000000000000000042

)

SELECT * FROM global_values
LIMIT 1 -- should never be more than 1 row, just confirm this