{{
  config(

    alias='vrf_request_fulfilled_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.date_start')],
    unique_key=['date_start', 'operator_address']
  )
}}

{%- set incremental_lower_bound -%}
date_trunc('{{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }}', now() - interval '{{ var("DBT_ENV_INCREMENTAL_TIME") }}' {{ var("DBT_ENV_INCREMENTAL_TIME_UNIT") }})
{%- endset -%}

-- The upstream view chainlink_polygon_vrf_request_fulfilled computes
-- evt_block_time = MAX(block_time) above a GROUP BY, so filtering its output
-- with incremental_predicate('evt_block_time') cannot reach the polygon.logs
-- scans (3 full scans, ~2.4 TiB/run, to merge a handful of rows). The view
-- body is inlined here so the time bounds apply BELOW the aggregations and
-- prune the Delta scans via block_time file skipping.
WITH vrf_request_fulfilled AS (
  SELECT
    'polygon' as blockchain,
    MAX(bytearray_to_uint256(bytearray_substring(v1_request.data, 110, 19)) / 1e18) AS token_value,
    MAX(v1_fulfilled.tx_from) as operator_address,
    MAX(v1_fulfilled.block_time) as evt_block_time
  FROM
    {{ ref('chainlink_polygon_vrf_v1_random_request_logs') }} v1_request
    INNER JOIN {{ ref('chainlink_polygon_vrf_v1_random_fulfilled_logs') }} v1_fulfilled ON bytearray_substring(v1_fulfilled.data, 1, 32) = bytearray_substring(v1_request.data, 129, 32)
  {% if is_incremental() %}
  -- The request side is a materialized ~6.3M-row table read in full, so no
  -- request-side lookback (or delay assumption) is needed. The fulfilled-side
  -- bound is exact: a group passes the post-agg evt_block_time predicate iff
  -- its MAX(fulfilled.block_time) row is itself in the window, and dropping
  -- older sibling fulfillments cannot change MAX() or the request-side values.
  WHERE {{ incremental_predicate('v1_fulfilled.block_time') }}
  {% endif %}
  GROUP BY
    v1_request.tx_hash,
    v1_fulfilled.tx_from

  UNION

  SELECT
    'polygon' as blockchain,
    MAX(bytearray_to_uint256(bytearray_substring(v2_fulfilled.data, 33, 32)) / 1e18) AS token_value,
    MAX(v2_fulfilled.tx_from) as operator_address,
    MAX(v2_fulfilled.block_time) as evt_block_time
  FROM
    {{ ref('chainlink_polygon_vrf_v2_random_fulfilled_logs') }} v2_fulfilled
  {% if is_incremental() %}
  -- exact: each group is a single log row (tx_hash + index), so the pre-agg
  -- bound is equivalent to the post-agg evt_block_time predicate
  WHERE {{ incremental_predicate('v2_fulfilled.block_time') }}
  {% endif %}
  GROUP BY
    v2_fulfilled.tx_hash,
    v2_fulfilled.tx_from,
    v2_fulfilled.index
)

SELECT
  'polygon' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  vrf_request_fulfilled.operator_address,
  SUM(token_value) as token_amount
FROM
  vrf_request_fulfilled
{% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}
GROUP BY
  2, 4
ORDER BY
  2, 4
