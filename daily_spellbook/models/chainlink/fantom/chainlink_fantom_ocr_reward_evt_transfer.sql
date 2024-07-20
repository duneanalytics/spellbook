{{
  config(
    alias='ocr_reward_evt_transfer',
    materialized='view'
  )
}}

WITH
  ocr_reward_evt_transfer AS (
    {{
        chainlink_ocr_reward_evt_transfer(
            blockchain = 'fantom'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_evt_transfer
