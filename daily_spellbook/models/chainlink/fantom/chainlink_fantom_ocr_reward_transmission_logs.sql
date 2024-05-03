{{
  config(
    alias='ocr_reward_transmission_logs',
    materialized='view',
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

WITH
  ocr_reward_transmission_logs AS (
    {{
        chainlink_ocr_reward_transmission_logs(
            blockchain = 'fantom'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reward_transmission_logs