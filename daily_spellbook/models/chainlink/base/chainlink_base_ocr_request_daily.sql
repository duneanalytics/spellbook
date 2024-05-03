{{
  config(
    alias='ocr_request_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'node_address'],
    post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}

WITH
  ocr_request_daily AS (
    {{
        chainlink_ocr_request_daily(
            blockchain = 'base'
        )
    }}
  )
SELECT
    *
FROM
    ocr_request_daily