{{
  config(
    schema='chainlink_arbitrum',
    alias='ccip_tokens_transferred',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['blockchain', 'tx_hash', 'total_tokens', 'token_symbol']
  )
}}


WITH
transferred AS (
    {{
        chainlink_ccip_tokens_transferred(
            blockchain = 'arbitrum'
        )
    }}
)

SELECT
    *
FROM
    transferred