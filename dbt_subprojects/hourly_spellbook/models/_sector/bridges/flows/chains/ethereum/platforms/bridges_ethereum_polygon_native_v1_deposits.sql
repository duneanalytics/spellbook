{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'polygon_native_v1_deposits',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, 137 AS withdrawal_chain_id
, 'polygon' AS withdrawal_chain
, 'Polygon' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, depositor AS sender
, depositReceiver AS recipient
, 'erc20' AS deposit_token_standard
, rootToken AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('matic_ethereum', 'erc20predicate_evt_lockederc20') }}

UNION ALL

SELECT 'ethereum' AS deposit_chain
, 137 AS withdrawal_chain_id
, 'polygon' AS withdrawal_chain
, 'Polygon' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, depositor AS sender
, depositReceiver AS recipient
, 'native' AS deposit_token_standard
, 0x0000000000000000000000000000000000000000 AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('polygon_ethereum', 'etherpredicate_evt_lockedether') }}