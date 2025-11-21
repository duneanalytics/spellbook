{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'avalanche_c_native_v2_withdrawals',
    materialized = 'view',
    )
}}

SELECT 'avalanche_c' AS deposit_chain
, CAST(43114 AS DOUBLE) AS deposit_chain_id
, 'ethereum' AS withdrawal_chain
, 'Avalanche' AS bridge_name
, '2' AS bridge_version
, block_date AS block_date
, block_time AS block_time
, block_number AS block_number
, amount_raw AS withdrawal_amount_raw
, "to" AS sender
, "to" AS recipient
, token_standard AS withdrawal_token_standard
, 0xaf88d065e77c8cc2239327c5edb3a432268e5831 AS withdrawal_token_address
, tx_from AS tx_from
, tx_hash AS tx_hash
, COALESCE(evt_index, 0) AS evt_index
, contract_address
, unique_key AS bridge_transfer_id
--, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('tokens_ethereum', 'transfers') }}
WHERE "from" = 0x8eb8a3b98659cce290402893d0123abb75e3ab28
AND block_number >= 5096229