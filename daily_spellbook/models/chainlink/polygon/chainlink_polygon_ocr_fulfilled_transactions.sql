{{
  config(

    alias='ocr_fulfilled_transactions',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

WITH
ocr_fulfilled_transactions AS (
    {{
        chainlink_ocr_fulfilled_transactions(
            blockchain = 'polygon',
            gas_token_symbol = 'MATIC',
            gas_price_column = 'gas_price',
        )
    }}
)

SELECT
    *
FROM
    ocr_fulfilled_transactions