{{
  config(
    
    alias='automation_upkeep_performed_logs',
    materialized='view',
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}

SELECT
  'fantom' as blockchain,
  logs.block_hash,
  logs.contract_address,
  logs.data,
  logs.topic0,
  logs.topic1,
  logs.topic2,
  logs.topic3,
  logs.tx_hash,
  logs.block_number,
  logs.block_time,
  logs.index,
  logs.tx_index,
  fantom_tx."from" as tx_from
FROM
  {{ source('fantom', 'logs') }} logs
LEFT JOIN {{ source('fantom', 'transactions') }} fantom_tx ON fantom_tx.hash = logs.tx_hash
WHERE
  topic0 = 0xcaacad83e47cc45c280d487ec84184eee2fa3b54ebaa393bda7549f13da228f6 -- UpkeepPerformed
OR
  topic0 = 0xad8cc9579b21dfe2c2f6ea35ba15b656e46b4f5b0cb424f52739b8ce5cac9c5b -- UpkeepPerformedV2