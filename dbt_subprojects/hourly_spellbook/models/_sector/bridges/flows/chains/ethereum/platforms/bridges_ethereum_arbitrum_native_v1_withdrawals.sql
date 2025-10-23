{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'across_v2_withdrawals',
    materialized = 'view',
    )
}}

SELECT 42161 AS deposit_chain_id
, 'ethereum' AS deposit_chain
, NULL AS withdrawal_chain
, 'Arbitrum' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, _amount AS deposit_amount_raw
, _from AS sender
, _to AS recipient
, 'erc20' AS deposit_token_standard
, l1Token AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS  tx_hash
, evt_index
, contract_address
, _exitNum AS bridge_id
FROM {{ source('arbitrum_ethereum', 'l1erc20gateway_evt_withdrawalfinalized') }}