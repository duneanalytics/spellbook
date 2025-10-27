{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'zksync_native_v2_withdrawals',
    materialized = 'view',
    )
}}

SELECT 324 AS deposit_chain_id
, 'zksync' AS deposit_chain
, 'ethereum' AS withdrawal_chain
, 'zkSync' AS bridge_name
, '2' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS withdrawal_amount_raw
, to AS sender
, to AS recipient
, 'native' AS withdrawal_token_standard
, 0x0000000000000000000000000000000000000000 AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('zksync_v2_ethereum', 'diamondproxy_evt_ethwithdrawalfinalized') }}

UNION ALL

SELECT 324 AS deposit_chain_id
, 'zksync' AS deposit_chain
, 'ethereum' AS withdrawal_chain
, 'zkSync' AS bridge_name
, '2' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS withdrawal_amount_raw
, to AS sender
, to AS recipient
, 'erc20' AS withdrawal_token_standard
, l1Token AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('zksync_v2_ethereum', 'l1erc20bridge_evt_withdrawalfinalized') }} d