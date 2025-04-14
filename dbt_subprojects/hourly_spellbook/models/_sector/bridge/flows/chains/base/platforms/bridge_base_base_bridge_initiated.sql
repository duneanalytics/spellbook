{% set blockchain = 'base' %}

{{ config(
    schema = 'bridge_' + blockchain,
    alias = 'base_initiated',
    materialized = 'view',
    )
}}

SELECT 'base' AS source_blockchain
, 'ethereum' AS destination_blockchain
, 'Base Bridge' AS project
, '1' AS project_version
, 'source' AS event_side
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS amount_raw
, "from" AS source_address
, to AS destination_address
, 'erc20' AS source_token_standard
, 'erc20' AS destination_token_standard
, localtoken AS source_token_address
, remotetoken AS destination_token_address
, extraData AS extra_data
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{ source('ovm_base', 'l2standardbridge_evt_erc20bridgeinitiated')}}

UNION ALL

SELECT 'base' AS source_blockchain
, 'ethereum' AS destination_blockchain
, 'Base Bridge' AS project
, '1' AS project_version
, 'source' AS event_side
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, amount AS amount_raw
, "from" AS source_address
, to AS destination_address
, 'native' AS source_token_standard
, 'native' AS destination_token_standard
, NULL AS source_token_address
, NULL AS destination_token_address
, extraData AS extra_data
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
FROM {{ source('ovm_base', 'l2standardbridge_evt_ethbridgeinitiated')}}