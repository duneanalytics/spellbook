{{
  config(
    
    alias='ocr_gas_transmission_logs',
    materialized='view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

WITH
ocr_gas_transmission_logs AS (
    {{
        chainlink_ocr_gas_transmission_logs(
            blockchain = 'ethereum'
        )
    }}
)

SELECT
    *
FROM
    ocr_gas_transmission_logs