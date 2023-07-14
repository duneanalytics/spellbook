{{
  config(
    tags=['dunesql'],
    alias='ocr_gas_transmission_logs',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}

SELECT
  'gnosis' as blockchain,
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
  {{ source('gnosis', 'logs') }} logs
WHERE
  topic0 = 0xf6a97944f31ea060dfde0566e4167c1a1082551e64b60ecb14d599a9d023d451
  {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
  {% endif %}      
