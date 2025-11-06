{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'rainbow_v1_deposits',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, CASE WHEN starts_with(accountId, 'aurora:') THEN 1313161554 ELSE 397 END AS withdrawal_chain_id
, CASE WHEN starts_with(accountId, 'aurora:') THEN 'aurora' ELSE 'near' END AS withdrawal_chain
, 'Rainbow Bridge' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, sender
, accountId AS recipient
, token AS deposit_token_address
, token AS withdrawal_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(['evt_tx_hash', 'evt_index']) }} as bridge_transfer_id
FROM {{ source('near_ethereum', 'erc20locker_evt_locked') }}