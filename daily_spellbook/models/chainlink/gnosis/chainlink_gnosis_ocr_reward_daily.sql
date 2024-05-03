{{
  config(
    alias='ocr_reward_daily',
    partition_by = ['date_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['date_start', 'admin_address']
  )
}}

WITH
  ocr_reward_daily AS (
    {{
        chainlink_ocr_reward_daily(
            blockchain = 'gnosis'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_daily
