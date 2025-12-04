{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'lighter_v1_withdrawals',
    materialized = 'view',
    )
}}

SELECT 'lighter' AS deposit_chain
, CAST(NULL AS DOUBLE) AS deposit_chain_id
, 'ethereum' AS withdrawal_chain
, 'Lighter' AS bridge_name
, '1' AS bridge_version
, block_date AS block_date
, block_time AS block_time
, block_number AS block_number
, amount_raw AS withdrawal_amount_raw
, "to" AS sender
, "to" AS recipient
, token_standard AS withdrawal_token_standard
, contract_address AS withdrawal_token_address
, tx_from AS tx_from
, tx_hash AS tx_hash
, COALESCE(evt_index, 0) AS evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('tokens_ethereum', 'transfers') }}
WHERE "from" = 0x3b4d794a66304f130a4db8f2551b0070dfcf5ca7
    AND contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
    AND block_number >= 21642011