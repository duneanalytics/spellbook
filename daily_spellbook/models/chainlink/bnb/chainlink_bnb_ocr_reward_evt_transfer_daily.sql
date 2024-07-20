{{
  config(
    alias='ocr_reward_evt_transfer_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.date_start')],
    unique_key=['date_start', 'admin_address']
  )
}}

WITH
  ocr_reward_evt_transfer_daily AS (
    {{
        chainlink_ocr_reward_evt_transfer_daily(
            blockchain = 'bnb'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_evt_transfer_daily
