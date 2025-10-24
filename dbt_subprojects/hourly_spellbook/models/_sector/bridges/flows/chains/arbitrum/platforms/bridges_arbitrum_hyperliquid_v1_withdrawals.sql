{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'hyperliquid_v1_withdrawals',
    materialized = 'view',
    )
}}

SELECT CAST(NULL AS DOUBLE) AS deposit_chain_id
, 'hyperliquid' AS deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'Hyperliquid' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, CAST(json_extract_scalar(e, '$.usdc') AS BIGINT) AS withdrawal_amount_raw
, from_hex(json_extract_scalar(e, '$.user')) AS sender
, from_hex(json_extract_scalar(e, '$.user')) AS recipient
, 'erc20' AS withdrawal_token_standard
, 0xff970a61a04b1ca14834a43f5de4533ebddb5cc8 AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('hyperliquid_arbitrum', 'hyperliquid_bridge_evt_withdraw') }} d