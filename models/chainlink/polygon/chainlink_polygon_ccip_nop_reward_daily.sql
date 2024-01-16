{{
  config(
    
    alias='ccip_nop_reward_daily',
    partition_by = ['date_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['date_start', 'nop_address']
  )
}}

{% set incremental_interval = '7' %}

WITH
  link_usd_daily AS (
    SELECT
      cast(date_trunc('day', price.minute) as date) as "date_start",
      MAX(price.price) as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      price.symbol = 'LINK'     
    GROUP BY
      1
    ORDER BY
      1
  ),
  nop_paid AS (
    SELECT
  'polygon' as blockchain,
  cast(date_trunc('day', block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', block_time) AS date)) AS date_month,
  nop_logs.nop_address as nop_address,
  MAX(admin_meta.operator_name) as operator_name,
  SUM(nop_logs.total_tokens / 1e18) as token_amount
FROM
  {{ref('chainlink_polygon_ccip_nop_paid_logs')}} nop_logs
  LEFT JOIN {{ref('chainlink_polygon_ccip_admin_meta')}} admin_meta ON admin_meta.admin_address = nop_logs.nop_address
{% if is_incremental() %}
  WHERE block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}      
GROUP BY
  2, 4
ORDER BY
  2, 4
  ),
  nop_reward_daily AS (
    SELECT 
      nop_paid.date_start,
      cast(date_trunc('month', nop_paid.date_start) as date) as date_month,
      nop_paid.operator_name,
      nop_paid.nop_address,   
      nop_paid.token_amount as token_amount,
      (nop_paid.token_amount * lud.usd_amount) as usd_amount
    FROM 
      nop_paid
    LEFT JOIN link_usd_daily lud ON lud.date_start = nop_paid.date_start
    ORDER BY date_start
  )
SELECT
  'polygon' as blockchain,
  date_start,
  date_month,
  operator_name,
  nop_address,
  token_amount,
  usd_amount
FROM 
  nop_reward_daily
ORDER BY
  2, 5
