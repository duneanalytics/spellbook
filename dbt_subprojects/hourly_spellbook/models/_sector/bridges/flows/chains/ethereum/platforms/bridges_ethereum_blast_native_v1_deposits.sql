{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'blast_native_v1_deposits',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, 81457 AS withdrawal_chain_id
, 'blast' AS withdrawal_chain
, 'Blast' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, user AS sender
, user AS recipient
, 0x0000000000000000000000000000000000000000 AS deposit_token_address
, 'native' AS deposit_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('blast_ethereum', 'launchbridge_evt_ethdeposited') }} d

UNION ALL

SELECT 'ethereum' AS deposit_chain
, 81457 AS withdrawal_chain_id
, 'blast' AS withdrawal_chain
, 'Blast' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, daiAmount AS deposit_amount_raw
, user AS sender
, user AS recipient
, 0x6b175474e89094c44da98b954eedeac495271d0f AS deposit_token_address
, 'erc20' AS deposit_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('blast_ethereum', 'launchbridge_evt_usddeposited') }}