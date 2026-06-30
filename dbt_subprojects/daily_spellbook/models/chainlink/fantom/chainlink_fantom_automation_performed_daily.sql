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

-- The upstream view chainlink_fantom_automation_performed computes
-- evt_block_time = MAX(block_time) above a GROUP BY, so filtering its output with
-- incremental_predicate('evt_block_time') cannot reach the fantom.logs scan and
-- forces a full-history scan (~52B rows/run) to merge a handful of rows. The view
-- body is inlined here so the incremental time bound applies BELOW the aggregation
-- and prunes the Delta scan via block_time file skipping. Each (tx_hash, index,
-- tx_from) group is a single log, so MAX(block_time) = block_time and the pre-agg
-- bound selects exactly the same groups as the post-agg evt_block_time predicate
-- (kept as authoritative).
WITH automation_performed AS (
  SELECT
    'fantom' as blockchain,
    MAX(operator_name) as operator_name,
    MAX(COALESCE(keeper_address, automation_logs.tx_from)) as keeper_address,
    MAX(automation_logs.block_time) as evt_block_time,
    MAX(bytearray_to_uint256(bytearray_substring(data, 21, 12)) / 1e18) as token_value
  FROM
    {{ ref('chainlink_fantom_automation_upkeep_performed_logs') }} automation_logs
    LEFT JOIN {{ ref('chainlink_fantom_automation_meta') }} automation_meta ON automation_meta.keeper_address = automation_logs.tx_from
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('automation_logs.block_time') }}
  {% endif %}
  GROUP BY
    tx_hash,
    index,
    tx_from
)

SELECT
  'fantom' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  automation_performed.keeper_address as keeper_address,
  MAX(automation_performed.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  automation_performed
{% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}
GROUP BY
  2, 4
ORDER BY
  2, 4
