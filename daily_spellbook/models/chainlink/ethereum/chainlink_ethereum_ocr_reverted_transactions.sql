{{
  config(
    alias='ocr_reverted_transactions',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
  )
}}

WITH
  ocr_reverted_transactions AS (
    {{
        chainlink_ocr_reverted_transactions(
            blockchain = 'ethereum',
            gas_token_symbol = 'ETH'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reverted_transactions