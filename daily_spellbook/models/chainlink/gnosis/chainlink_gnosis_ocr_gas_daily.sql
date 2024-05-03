{{
  config(
    alias='ocr_gas_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'node_address']
  )
}}

WITH
  ocr_gas_daily AS (
    {{
        chainlink_ocr_gas_daily(
            blockchain = 'gnosis'
        )
    }}
  )
SELECT
    *
FROM
    ocr_gas_daily
