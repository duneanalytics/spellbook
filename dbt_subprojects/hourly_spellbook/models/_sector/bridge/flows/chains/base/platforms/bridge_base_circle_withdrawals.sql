{% set blockchain = 'base' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'circle_withdrawals',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, 'base' AS withdraw_chain
, 'Circle' AS project
, '1' AS project_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, amount AS withdraw_amount_raw
, "from" AS sender
, to AS recipient
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdraw_token_standard
, remotetoken AS deposit_token_address
, localtoken AS withdraw_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(NULL AS varbinary) AS bridge_id
FROM {{ source('ovm_base', 'l2standardbridge_evt_erc20bridgefinalized')}}

UNION ALL

SELECT 'ethereum' AS deposit_chain
, 'base' AS withdraw_chain
, 'Base Bridge' AS project
, '1' AS project_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS deposit_amount_raw
, amount AS withdraw_amount_raw
, "from" AS sender
, to AS recipient
, 'native' AS deposit_token_standard
, 'native' AS withdraw_token_standard
, NULL AS deposit_token_address
, NULL AS withdraw_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(["Base Bridge'" "1", 'deposit_chain', 'withdraw_chain', '"from"', 'to', 'localtoken', 'remotetoken', 'amount', 'amount'])}} as bridge_id
FROM {{ source('ovm_base', 'l2standardbridge_evt_ethbridgefinalized')}}