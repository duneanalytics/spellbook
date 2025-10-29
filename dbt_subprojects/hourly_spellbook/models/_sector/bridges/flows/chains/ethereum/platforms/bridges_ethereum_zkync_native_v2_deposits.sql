{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'zksync_native_v2_deposits',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, 324 AS withdrawal_chain_id
, 'zksync' AS withdrawal_chain
, 'zkSync' AS bridge_name
, '2' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, "from" AS sender
, to AS recipient
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, l1Token AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(l2DepositTxHash AS varchar) AS bridge_transfer_id
FROM {{ source('zksync_v2_ethereum', 'l1erc20bridge_evt_depositinitiated') }} d