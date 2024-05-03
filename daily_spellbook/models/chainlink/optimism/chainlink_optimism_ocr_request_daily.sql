{{
  config(
    alias='ocr_request_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'node_address']
  )
}}

WITH
  ocr_request_daily AS (
    {{
        chainlink_ocr_request_daily(
            blockchain = 'optimism'
        )
    }}
  )
SELECT
    *
FROM
    ocr_request_daily