{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_reward_transmission_logs'),
    materialized='view',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

SELECT
  'arbitrum' as blockchain,
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
  tx_index
FROM
  {{ source('arbitrum', 'logs') }} logs
WHERE
  topic0 = 0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6