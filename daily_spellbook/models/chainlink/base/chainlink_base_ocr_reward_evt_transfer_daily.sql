{{
  config(
    alias='ocr_reward_evt_transfer_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'admin_address'],
    post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}

WITH
  ocr_reward_evt_transfer_daily AS (
    {{
        chainlink_ocr_reward_evt_transfer_daily(
            blockchain = 'base'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_evt_transfer_daily
