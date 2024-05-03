{{
  config(
    alias='ocr_reward_evt_transfer',
    materialized='view',
    post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}

WITH
  ocr_reward_evt_transfer AS (
    {{
        chainlink_ocr_reward_evt_transfer(
            blockchain = 'base'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_evt_transfer
