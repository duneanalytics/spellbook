{% set blockchain = 'avalanche_c' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'avalanche_c_bitcoin_v1_deposits',
    materialized = 'view',
    )
}}


SELECT 'avalanche_c' AS deposit_chain
, chainId AS withdrawal_chain_id
, 'bitcoin' AS withdrawal_chain
, 'Avalanche' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, evt_tx_from AS sender
, CAST(NULL AS varbinary) AS recipient
, 'erc20' AS deposit_token_standard
, 0x152b9d0fdc40c096757f570a51e494bd4b943e50 AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} AS bridge_transfer_id
FROM {{ source('btcb_avalanche_c', 'bridgetoken_evt_unwrap') }}