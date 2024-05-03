{{
  config(
    alias='ocr_reward_evt_transfer',
    materialized='view',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

WITH
  ocr_reward_evt_transfer AS (
    {{
        chainlink_ocr_reward_evt_transfer(
            blockchain = 'optimism'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_evt_transfer
