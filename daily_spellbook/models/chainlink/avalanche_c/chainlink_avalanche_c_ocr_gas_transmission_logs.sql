{{
  config(
    
    alias='ocr_gas_transmission_logs',
    materialized='view',
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

WITH
ocr_gas_transmission_logs AS (
    {{
        chainlink_ocr_gas_transmission_logs(
            blockchain = 'avalanche_c'
        )
    }}
)

SELECT
    *
FROM
    ocr_gas_transmission_logs