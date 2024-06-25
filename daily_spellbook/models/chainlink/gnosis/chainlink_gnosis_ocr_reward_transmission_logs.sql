{{
  config(
    alias='ocr_reward_transmission_logs',
    materialized='view'
  )
}}

WITH
  ocr_reward_transmission_logs AS (
    {{
        chainlink_ocr_reward_transmission_logs(
            blockchain = 'gnosis'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_transmission_logs