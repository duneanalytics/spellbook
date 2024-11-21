{{
  config(

    alias='automation_performed_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'keeper_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.date_start')]
  )
}}


SELECT
  'bnb' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  automation_performed.keeper_address as keeper_address,
  MAX(automation_performed.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  {{ref('chainlink_bnb_automation_performed')}} automation_performed
{% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}
GROUP BY
  2, 4
ORDER BY
  2, 4
