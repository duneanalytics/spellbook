{{
  config(
    tags=['dunesql'],
    alias=alias('vrf_reward_daily'),
    partition_by = ['date_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['date_start', 'operator_address']
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
  vrf_reward_daily AS (
    SELECT 
      vrf_daily.date_start,
      cast(date_trunc('month', vrf_daily.date_start) as date) as date_month,
      vrf_daily.operator_address,      
      COALESCE(vrf_daily.token_amount, 0) as token_amount,
      COALESCE(vrf_daily.token_amount * lud.usd_amount, 0)  as usd_amount
    FROM 
      {{ref('chainlink_fantom_vrf_request_fulfilled_daily')}} vrf_daily
    LEFT JOIN link_usd_daily lud ON lud.date_start = vrf_daily.date_start
    ORDER BY date_start
  )
SELECT
  'fantom' as blockchain,
  date_start,
  date_month,
  operator_address,
  token_amount,
  usd_amount
FROM 
  vrf_reward_daily
ORDER BY
  2, 4
