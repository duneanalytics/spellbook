{{
  config(
    alias='ccip_reverted_transactions',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'caller_address']
  )
}}


WITH
  ccip_reverted_transactions AS (
    SELECT
      ccip_send_logs_v1.tx_hash as tx_hash,
      ccip_send_logs_v1.block_time as block_time,
      cast(date_trunc('day', ccip_send_logs_v1.block_time) as date) as date_start,
      ccip_send_logs_v1.tx_from as caller_address,
      ccip_send_logs_v1.tx_index as tx_index
    FROM
      {{ ref('chainlink_base_ccip_send_requested_logs_v1') }} ccip_send_logs_v1
      LEFT JOIN {{ source('base', 'transactions') }} tx ON
        ccip_send_logs_v1.tx_hash = tx.hash
        {% if is_incremental() %}
            AND {{ incremental_predicate('tx.block_time') }}
        {% endif %}
      WHERE
        tx.success = false
      {% if is_incremental() %}
        AND {{ incremental_predicate('ccip_send_logs_v1.block_time') }}
      {% endif %}

    UNION

    SELECT
      ccip_send_logs_v1_2.tx_hash as tx_hash,
      ccip_send_logs_v1_2.block_time as block_time,
      cast(date_trunc('day', ccip_send_logs_v1_2.block_time) as date) as date_start,
      ccip_send_logs_v1_2.tx_from as caller_address,
      ccip_send_logs_v1_2.tx_index as tx_index
    FROM
      {{ ref('chainlink_base_ccip_send_requested_logs_v1_2') }} ccip_send_logs_v1_2
      LEFT JOIN {{ source('base', 'transactions') }} tx ON
        ccip_send_logs_v1_2.tx_hash = tx.hash
        {% if is_incremental() %}
            AND {{ incremental_predicate('tx.block_time') }}
        {% endif %}
      WHERE
        tx.success = false
      {% if is_incremental() %}
        AND {{ incremental_predicate('ccip_send_logs_v1_2.block_time') }}
      {% endif %}

  )
SELECT
 'base' as blockchain,
  block_time,
  date_start,
  caller_address,
  tx_hash,
  tx_index
FROM
  ccip_reverted_transactions