{{
  config(
    schema = 'polymarket_polygon',
    alias = 'protocol_tvl',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['hour']
  )
}}

WITH latest_transfer_hour AS (
  SELECT date_trunc('hour', MAX(block_time)) AS max_hour
  FROM {{ source('tokens_polygon', 'transfers') }}
  WHERE block_time > NOW() - interval '3' day
  )


{% if is_incremental() %}
, last_spell_update AS (
  SELECT MAX(hour) AS max_hour
  , MAX_BY(tvl, hour) AS latest_tvl
  FROM {{this}}
  )
{% endif %}

, flows AS (
  SELECT date_trunc('hour', block_time) AS hour
  , SUM(amount) as flow
  FROM {{ source('tokens_polygon', 'transfers') }} t
  WHERE contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND to IN (0x4d97dcd97ec945f40cf65f87097ace5ea0476045, 0x3a3bd7bb9528e159577f7c2e685cc81a765002e2)
  AND block_time > (SELECT max_hour FROM latest_transfer_hour)
  AND block_number > 4023686
  {% if is_incremental() %}
  AND block_time > (SELECT max_hour FROM last_spell_update)
  {% endif %}
  GROUP BY 1

  UNION ALL

  SELECT date_trunc('hour', block_time) AS hour
  , -SUM(amount) as flow
  FROM {{ source('tokens_polygon', 'transfers') }} t
  WHERE contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- USDC.e
  AND "from" IN (0x4d97dcd97ec945f40cf65f87097ace5ea0476045, 0x3a3bd7bb9528e159577f7c2e685cc81a765002e2)
  AND block_time > (SELECT max_hour FROM latest_transfer_hour)
  AND block_number > 4023686
  {% if is_incremental() %}
  AND block_time > (SELECT max_hour FROM last_spell_update)
  {% endif %}
  GROUP BY 1
  )


{% if is_incremental() %}

SELECT f.hour
, SUM(f.flow) AS net_flow
, SUM(SUM(lu.latest_tvl + f.flow)) OVER (ORDER BY f.hour) AS tvl
FROM flows f
INNER JOIN last_update lu ON f.hour > lu.max_hour
GROUP BY 1

{% else %}

SELECT hour
, SUM(flow) AS net_flow
, SUM(SUM(flow)) OVER (ORDER BY hour) AS tvl
FROM flows
GROUP BY 1

{% endif %}