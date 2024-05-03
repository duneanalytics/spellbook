{{
  config(
    alias='ocr_reward_transmission_logs',
    materialized='view',
    post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}

WITH
  ocr_reward_transmission_logs AS (
    {{
        chainlink_ocr_reward_transmission_logs(
            blockchain = 'base'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_transmission_logs