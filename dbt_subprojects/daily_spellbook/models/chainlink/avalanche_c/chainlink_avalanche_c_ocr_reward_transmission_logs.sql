{{
  config(
    
    alias='ocr_reward_transmission_logs',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'index']
    , post_hook='{{ hide_spells() }}'
  )
}}

-- Performance (CUR2-2973): materialized as an incremental table instead of a view so the
-- full <chain>.logs scan by topic0 (tens of billions of rows) is not repeated on every
-- downstream run. The NewTransmission set is append-only, so the table accumulates the
-- all-history reward-distributor contract set; incremental runs only harvest new logs.

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
  tx_index
FROM
  {{ source('avalanche_c', 'logs') }} logs
WHERE
  topic0 = 0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6
{% if is_incremental() %}
  AND {{ incremental_predicate('block_time') }}
{% endif %}
