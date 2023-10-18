{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_reconcile_daily'),
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

WITH
  reconcile_20231017_polygon_evt_transfer as (
    SELECT
      evt_transfer."from" as admin_address,
      MAX(cast(evt_transfer.value as double) / 1e18) as token_value
    FROM
      {{ source('erc20_polygon', 'evt_Transfer') }} evt_transfer
    LEFT JOIN
      {{ ref('chainlink_polygon_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = evt_transfer."from"
    WHERE
      evt_transfer.evt_block_time >= cast('2023-10-16' as date)
      AND evt_transfer."to" = 0x2431d49d225C1BcCE7541deA6Da7aEf9C7AD3e23    
    GROUP BY
      evt_transfer.evt_tx_hash,
      evt_transfer.evt_index,
      evt_transfer."from"
  ),
  reconcile_20231017_polygon_daily as (
    SELECT
      cast('2023-10-16' AS date) AS date_start,
      cast(date_trunc('month', cast('2023-10-16' as date)) as date) as date_month,
      admin_address,
      0 - SUM(token_value) as token_amount
    FROM
      reconcile_20231017_polygon_evt_transfer
    GROUP BY
      3
  ),
  reconcile_20231017_ethereum_evt_transfer as (
    SELECT
      evt_transfer."from" as admin_address,
      MAX(cast(evt_transfer.value as double) / 1e18) as token_value
    FROM
      {{ source('erc20_ethereum', 'evt_Transfer') }} evt_transfer
    LEFT JOIN
      {{ ref('chainlink_ethereum_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = evt_transfer."from"
    WHERE
      evt_transfer.evt_block_time >= cast('2023-10-16' as date)
      AND evt_transfer."to" = 0xC489244f2a5FC0E65A0677560EAA4A13F5036ab6    
    GROUP BY
      evt_transfer.evt_tx_hash,
      evt_transfer.evt_index,
      evt_transfer."from"
  ),
  reconcile_20231017_ethereum_daily as (
    SELECT
      cast('2023-10-16' AS date) AS date_start,
      cast(date_trunc('month', cast('2023-10-16' as date)) as date) as date_month,
      admin_address,
      0 - SUM(token_value) as token_amount
    FROM
      reconcile_20231017_ethereum_evt_transfer
    GROUP BY
      3
  )
SELECT
  COALESCE(reconcile_polygon.date_start, reconcile_ethereum.date_start) as date_start,
  COALESCE(reconcile_polygon.admin_address, reconcile_ethereum.admin_address) as admin_address,
  COALESCE(reconcile_polygon.token_amount, 0) + COALESCE(reconcile_ethereum.token_amount, 0) as token_amount
FROM 
  reconcile_20231017_polygon_daily reconcile_polygon
FULL OUTER JOIN
  reconcile_20231017_ethereum_daily reconcile_ethereum ON reconcile_ethereum.admin_address = reconcile_polygon.admin_address