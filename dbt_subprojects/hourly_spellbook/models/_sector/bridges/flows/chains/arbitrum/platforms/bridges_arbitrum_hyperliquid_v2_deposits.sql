{% set blockchain = 'arbitrum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'hyperliquid_v2_deposits',
    materialized = 'view',
    )
}}

SELECT 'arbitrum' AS deposit_chain
, CAST(NULL AS DOUBLE) AS withdrawal_chain_id
, 'hyperliquid' AS withdrawal_chain
, 'Hyperliquid' AS bridge_name
, '2' AS bridge_version
, block_date AS block_date
, block_time AS block_time
, block_number AS block_number
, amount_raw AS deposit_amount_raw
, tx_from AS sender
, tx_from AS recipient
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, 0xaf88d065e77c8cc2239327c5edb3a432268e5831 AS deposit_token_address
, tx_from AS tx_from
, tx_hash AS tx_hash
, COALESCE(evt_index, 0) AS evt_index
, to AS contract_address
, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('tokens_arbitrum', 'transfers') }}
WHERE to = 0x2df1c51e09aecf9cacb7bc98cb1742757f163df7
AND contract_address = 0xaf88d065e77c8cc2239327c5edb3a432268e5831