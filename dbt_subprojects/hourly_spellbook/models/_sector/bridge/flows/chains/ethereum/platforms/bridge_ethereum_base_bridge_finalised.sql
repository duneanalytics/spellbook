{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'base_bridge_finalised',
    materialized = 'view',
    )
}}

SELECT 'base' AS source_chain
, 'ethereum' AS destination_chain
, 'Base Bridge' AS project
, '1' AS project_version
, 'source' AS event_side
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS source_amount_raw
, amount AS destination_amount_raw
, "from" AS source_address
, to AS destination_address
, 'erc20' AS source_token_standard
, 'erc20' AS destination_token_standard
, localtoken AS source_token_address
, remotetoken AS destination_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(NULL AS varbinary) AS bridge_id
FROM {{ source('base_ethereum', 'l1standardbridge_evt_erc20bridgefinalized')}}

UNION ALL

SELECT 'base' AS source_chain
, 'ethereum' AS destination_chain
, 'Base Bridge' AS project
, '1' AS project_version
, 'source' AS event_side
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS source_amount_raw
, amount AS destination_amount_raw
, "from" AS source_address
, to AS destination_address
, 'native' AS source_token_standard
, 'native' AS destination_token_standard
, NULL AS source_token_address
, NULL AS destination_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(NULL AS varbinary) AS bridge_id
FROM {{ source('base_ethereum', 'l1standardbridge_evt_ethbridgefinalized')}}