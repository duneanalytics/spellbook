{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'polygon_native_v1_withdrawals',
    materialized = 'view',
    )
}}

SELECT 'polygon' AS deposit_chain
, 137 AS deposit_chain_id
, 'ethereum' AS withdrawal_chain
, 'Polygon' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS withdrawal_amount_raw
, exitor AS sender
, exitor AS recipient
, 'native' AS withdrawal_token_standard
, 0x0000000000000000000000000000000000000000 AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('polygon_ethereum', 'etherpredicate_evt_exitedether') }}