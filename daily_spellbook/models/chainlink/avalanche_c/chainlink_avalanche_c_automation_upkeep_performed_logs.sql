{{
  config(
    
    alias='automation_upkeep_performed_logs',
    materialized='view',
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}

SELECT
  'avalanche_c' as blockchain,
  block_hash,
  contract_address,
  data,
  topic0,
  topic1,
  topic2,
  topic3,
  tx_hash,
  block_number,
  block_time,
  index,
  tx_index,
  tx_from
FROM
  {{ source('avalanche_c', 'logs') }} logs
WHERE
  topic0 = 0xcaacad83e47cc45c280d487ec84184eee2fa3b54ebaa393bda7549f13da228f6 -- UpkeepPerformed
OR
  topic0 = 0xad8cc9579b21dfe2c2f6ea35ba15b656e46b4f5b0cb424f52739b8ce5cac9c5b -- UpkeepPerformedV2