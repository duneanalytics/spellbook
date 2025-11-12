{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'starknet_native_v1_deposits',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, CAST(NULL AS DOUBLE) AS withdrawal_chain_id
, 'starknet' AS withdrawal_chain
, 'Starknet' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, sender AS sender
, l2Recipient AS recipient
, 'native' AS deposit_token_standard
, 0x0000000000000000000000000000000000000000 AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, COALESCE(evt_index, 0) AS evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('starknet_ethereum', 'starknetethbridge_evt_withdrawal') }}

UNION ALL

SELECT 'ethereum' AS withdrawal_chain
, CAST(NULL AS DOUBLE) AS deposit_chain_id
, 'starknet' AS deposit_chain
, 'Starknet' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS withdrawal_amount_raw
, recipient AS sender
, recipient AS recipient
, 'erc20' AS withdrawal_token_standard
, token AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, COALESCE(evt_index, 0) AS evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('starknet_ethereum', 'starknettokenbridge_evt_withdrawal') }}