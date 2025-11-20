{% set blockchain = 'avalanche_c' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'avalanche_c_bitcoin_v1_withdrawals',
    materialized = 'view',
    )
}}

SELECT 'avalanche_c' AS withdrawal_chain
, CAST(NULL AS BIGINT) AS deposit_chain_id
, 'bitcoin' AS deposit_chain
, 'Avalanche' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS withdrawal_amount_raw
, evt_tx_from AS sender
, to AS recipient
, 'erc20' AS withdrawal_token_standard
, 0x152b9d0fdc40c096757f570a51e494bd4b943e50 AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(originTxId AS varchar) as bridge_transfer_id
FROM {{ source('btcb_avalanche_c', 'bridgetoken_evt_mint') }}