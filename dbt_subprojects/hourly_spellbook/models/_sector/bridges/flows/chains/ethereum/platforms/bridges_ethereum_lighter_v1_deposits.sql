{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'lighter_v1_deposits',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, CAST(NULL AS DOUBLE) AS withdrawal_chain_id
, 'lighter' AS withdrawal_chain
, 'Lighter' AS bridge_name
, '1' AS bridge_version
, block_date AS block_date
, block_time AS block_time
, block_number AS block_number
, amount_raw AS deposit_amount_raw
, tx_from AS sender
, tx_from AS recipient
, token_standard AS deposit_token_standard
, token_standard AS withdrawal_token_standard
, contract_address AS deposit_token_address
, tx_from AS tx_from
, tx_hash AS tx_hash
, COALESCE(evt_index, 0) AS evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('tokens_ethereum', 'transfers') }}
WHERE to = 0x3b4d794a66304f130a4db8f2551b0070dfcf5ca7
    AND contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
    AND block_number >= 21642011