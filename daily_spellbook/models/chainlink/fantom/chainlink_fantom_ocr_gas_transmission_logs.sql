{{
  config(
    
    alias='ocr_gas_transmission_logs',
    materialized='view'
  )
}}

WITH
ocr_gas_transmission_logs AS (
    {{
        chainlink_ocr_gas_transmission_logs(
            blockchain = 'fantom'
        )
    }}
)

SELECT
    *
FROM
    ocr_gas_transmission_logs