{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'avalanche_c_native_v2_deposits',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, CAST(43114 AS DOUBLE) AS withdrawal_chain_id
, 'avalanche_c' AS withdrawal_chain
, 'Avalanche' AS bridge_name
, '2' AS bridge_version
, block_date AS block_date
, block_time AS block_time
, block_number AS block_number
, amount AS deposit_amount_raw
, tx_from AS sender
, tx_from AS recipient
, token_standard AS deposit_token_standard
, token_standard AS withdrawal_token_standard
, contract_address AS deposit_token_address
, tx_from AS tx_from
, tx_hash AS tx_hash
, COALESCE(evt_index, 0) AS evt_index
, contract_address
, unique_key AS bridge_transfer_id
--, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('tokens_ethereum', 'transfers') }}
WHERE to = 0x8eb8a3b98659cce290402893d0123abb75e3ab28
AND block_number >= 5096229