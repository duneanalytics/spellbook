{% set blockchain = 'base' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'base_bridge_finalised',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS source_chain
, 'base' AS destination_chain
, 'Base Bridge' AS project
, '1' AS project_version
, 'destination' AS event_side
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS source_amount_raw
, amount AS destination_amount_raw
, "from" AS sender
, to AS recipient
, 'erc20' AS source_token_standard
, 'erc20' AS destination_token_standard
, remotetoken AS source_token_address
, localtoken AS destination_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(NULL AS varbinary) AS bridge_id
FROM {{ source('ovm_base', 'l2standardbridge_evt_erc20bridgefinalized')}}

UNION ALL

SELECT 'ethereum' AS source_chain
, 'base' AS destination_chain
, 'Base Bridge' AS project
, '1' AS project_version
, 'destination' AS event_side
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS source_amount_raw
, amount AS destination_amount_raw
, "from" AS sender
, to AS recipient
, 'native' AS source_token_standard
, 'native' AS destination_token_standard
, NULL AS source_token_address
, NULL AS destination_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, {{ dbt_utils.generate_surrogate_key(["Base Bridge'" "1", 'source_chain', 'destination_chain', '"from"', 'to', 'localtoken', 'remotetoken', 'amount', 'amount'])}} as bridge_id
FROM {{ source('ovm_base', 'l2standardbridge_evt_ethbridgefinalized')}}