{{
  config(
    
    alias='ccip_send_requested',
    materialized='view'
  )
}}

WITH combined_logs AS (
  SELECT
    ccip_logs_v1.blockchain,
    ccip_logs_v1.block_time,
    ccip_logs_v1.fee_token_amount / 1e18 AS fee_token_amount,
    token_addresses.token_symbol AS token,
    ccip_logs_v1.fee_token,
    ccip_logs_v1.destination_selector,
    ccip_logs_v1.destination_blockchain,
    ccip_logs_v1.tx_hash
  FROM
    {{ ref('chainlink_optimism_ccip_send_requested_logs_v1') }} ccip_logs_v1
    LEFT JOIN {{ ref('chainlink_optimism_ccip_token_meta') }} token_addresses ON token_addresses.token_contract = ccip_logs_v1.fee_token

  UNION ALL

  SELECT
    ccip_logs_v1_2.blockchain,
    ccip_logs_v1_2.block_time,
    ccip_logs_v1_2.fee_token_amount / 1e18 AS fee_token_amount,
    token_addresses.token_symbol AS token,
    ccip_logs_v1_2.fee_token,
    ccip_logs_v1_2.destination_selector,
    ccip_logs_v1_2.destination_blockchain,
    ccip_logs_v1_2.tx_hash
  FROM
    {{ ref('chainlink_optimism_ccip_send_requested_logs_v1_2') }} ccip_logs_v1_2
    LEFT JOIN {{ ref('chainlink_optimism_ccip_token_meta') }} token_addresses ON token_addresses.token_contract = ccip_logs_v1_2.fee_token
)

SELECT
  MAX(blockchain) AS blockchain,
  MAX(block_time) AS evt_block_time,
  SUM(fee_token_amount) AS fee_token_amount,
  MAX(token) AS token,
  MAX(fee_token) AS fee_token,
  MAX(destination_selector) AS destination_selector,
  MAX(destination_blockchain) AS destination_blockchain
FROM
  combined_logs
GROUP BY
  tx_hash