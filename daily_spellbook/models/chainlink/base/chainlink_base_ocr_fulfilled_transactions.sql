{{
  config(
    
    alias='ocr_fulfilled_transactions',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address']
  )
}}

WITH
ocr_fulfilled_transactions AS (
    {{
        chainlink_ocr_fulfilled_transactions(
            blockchain = 'base',
            gas_token_symbol = 'ETH',
            gas_price_column = 'l1_fee',
        )
    }}
)

SELECT
    *
FROM
    ocr_fulfilled_transactions