{{
  config(
    tags=['dunesql'],
    alias=alias('automation_performed'),
    materialized='view'
  )
}}

SELECT
  'polygon' as blockchain,
  MAX(operator_name) as operator_name,
  MAX(COALESCE(keeper_address, automation_logs.tx_from)) as keeper_address,
  MAX(automation_logs.block_time) as evt_block_time,
  MAX(bytearray_to_uint256(bytearray_substring(data, 21, 12)) / 1e18) as token_value
FROM
  {{ ref('chainlink_polygon_automation_upkeep_performed_logs') }} automation_logs
  LEFT JOIN {{ ref('chainlink_polygon_automation_meta') }} automation_meta ON automation_meta.keeper_address = automation_logs.tx_from
GROUP BY
  tx_hash,
  index,
  tx_from