{{ config(
    alias = 'ocr_reward_daily',
    partition_by = ['date_start'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['date_start', 'admin_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
    )
}}

{% set incremental_interval = '1 week' %}

WITH
  admin_address_meta as (
    SELECT DISTINCT
      admin_address
    FROM
      {{ref('chainlink_ethereum_ocr_reward_evt_transfer_daily')}}
  ),
  link_usd_daily AS (
    SELECT
      date_trunc('day', minute) AS "date_start",
      MAX(price) as usd_amount
    FROM
      prices.usd
    WHERE
      symbol = 'LINK'
      {% if is_incremental() %}
        AND minute >= date_trunc("day", now() - interval '{{incremental_interval}}')
      {% endif %}      
    GROUP BY
      1
    ORDER BY
      1
  ),
  link_usd_daily_expanded_by_admin_address AS (
    SELECT
      date_start,
      usd_amount,
      admin_address
    FROM
      link_usd_daily
    CROSS JOIN
      admin_address_meta
    ORDER BY
      date_start,
      admin_address
  ),
  payment_meta AS (
    SELECT
      date_start,
      admin_address,
      usd_amount,
      (
        SELECT
          MAX(ocr_reward_evt_transfer_daily.date_start)
        FROM
          {{ref('chainlink_ethereum_ocr_reward_evt_transfer_daily')}}
        WHERE
          ocr_reward_evt_transfer_daily.date_start <= link_usd_daily_expanded_by_admin_address.date_start
          AND ocr_reward_evt_transfer_daily.admin_address = link_usd_daily_expanded_by_admin_address.admin_address
      ) as prev_payment_date,
      (
        SELECT
          MIN(ocr_reward_evt_transfer_daily.date_start)
        FROM
          {{ref('chainlink_ethereum_ocr_reward_evt_transfer_daily')}}
        WHERE
          ocr_reward_evt_transfer_daily.date_start > link_usd_daily_expanded_by_admin_address.date_start
          AND ocr_reward_evt_transfer_daily.admin_address = link_usd_daily_expanded_by_admin_address.admin_address
      ) as next_payment_date
    FROM
      link_usd_daily_expanded_by_admin_address
    ORDER BY
      1, 2
  ),
  ocr_reward_daily AS (
    SELECT 
      payment_meta.date_start,
      payment_meta.admin_address,
      operator_name,      
      COALESCE(ocr_reward_evt_transfer_daily.token_amount / EXTRACT(DAY FROM next_payment_date - prev_payment_date), 0) as token_amount,
      (COALESCE(ocr_reward_evt_transfer_daily.token_amount / EXTRACT(DAY FROM next_payment_date - prev_payment_date), 0) * payment_meta.usd_amount) as usd_amount
    FROM 
      payment_meta
    LEFT JOIN 
      {{ref('chainlink_ethereum_ocr_reward_evt_transfer_daily')}} ON
        payment_meta.next_payment_date = ocr_reward_evt_transfer_daily.date_start AND
        payment_meta.admin_address = ocr_reward_evt_transfer_daily.admin_address
    LEFT JOIN {{ ref('chainlink_ethereum_ocr_operator_admin_meta') }} ON LOWER(ocr_operator_admin_meta.admin_address) = LOWER(ocr_reward_evt_transfer_daily.admin_address.node_address)
    ORDER BY date_start
  )
SELECT
  'ethereum' as blockchain,
  date_start,
  admin_address,
  operator_name,
  token_amount,
  usd_amount,
FROM 
  ocr_reward_daily
ORDER BY
  2, 3
