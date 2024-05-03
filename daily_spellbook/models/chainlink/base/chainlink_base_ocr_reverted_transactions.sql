{{
  config(
    alias='ocr_reverted_transactions',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address'],
    post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

WITH
  ocr_reverted_transactions AS (
    {{
        chainlink_ocr_reverted_transactions(
            blockchain = 'base',
            gas_token_symbol = 'ETH'
        )
    }}
  )
SELECT
    *
FROM
    ocr_reverted_transactions