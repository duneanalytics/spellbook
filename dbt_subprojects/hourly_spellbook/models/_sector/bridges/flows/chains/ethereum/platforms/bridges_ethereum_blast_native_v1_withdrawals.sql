{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'blast_native_v1_withdrawals',
    materialized = 'view',
    )
}}

SELECT CAST(NULL AS DOUBLE) AS deposit_chain_id
, 'blast' AS deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'Blast' AS bridge_name
, '1' AS bridge_version
, block_date AS block_date
, block_time AS block_time
, block_number AS block_number
, amount AS withdrawal_amount_raw
, to AS sender
, to AS recipient
, 'erc20' AS withdrawal_token_standard
, contract_address AS withdrawal_token_address
, tx_from AS tx_from
, tx_hash AS tx_hash
, COALESCE(evt_index, 0) AS evt_index
, 0x5f6ae08b8aeb7078cf2f96afb089d7c9f51da47d AS contract_address
, {{ dbt_utils.generate_surrogate_key(['tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('tokens_ethereum', 'transfers') }}
WHERE "from" = 0x5f6ae08b8aeb7078cf2f96afb089d7c9f51da47d
AND contract_address IN (0xae7ab96520de3a18e5e111b5eaab095312d7fe84, 0x6b175474e89094c44da98b954eedeac495271d0f)
AND block_number >= 18617323