{{
  config(
    
    alias='automation_reward_daily',
    partition_by = ['date_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['date_start', 'keeper_address']
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
  automation_reward_daily AS (
    SELECT 
      automation_performed_daily.date_start,
      cast(date_trunc('month', automation_performed_daily.date_start) as date) as date_month,
      automation_performed_daily.operator_name,
      automation_performed_daily.keeper_address,   
      automation_performed_daily.token_amount as token_amount,
      (automation_performed_daily.token_amount * lud.usd_amount) as usd_amount
    FROM 
      {{ref('chainlink_fantom_automation_performed_daily')}} automation_performed_daily
    LEFT JOIN link_usd_daily lud ON lud.date_start = automation_performed_daily.date_start
    ORDER BY date_start
  )
SELECT
  'fantom' as blockchain,
  date_start,
  date_month,
  operator_name,
  keeper_address,
  token_amount,
  usd_amount
FROM 
  automation_reward_daily
ORDER BY
  2, 5
