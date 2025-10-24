{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'ronin_native_v2_deposits',
    materialized = 'view',
    )
}}

SELECT 'ethereum' AS deposit_chain
, 2020 AS withdrawal_chain_id
, 'ronin' AS withdrawal_chain
, 'Ronin' AS bridge_name
, '2' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, CAST(json_extract_scalar(json_extract_scalar(receipt, '$.info'), '$.quantity') AS UINT256) AS deposit_amount_raw
, from_hex(json_extract_scalar(json_extract_scalar(receipt, '$.mainchain'), '$.addr')) AS sender
, from_hex(json_extract_scalar(json_extract_scalar(receipt, '$.ronin'), '$.addr')) AS recipient
, CASE WHEN json_extract_scalar(json_extract_scalar(receipt, '$.mainchain'), '$.tokenAddr')='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 'native' ELSE 'erc20' END AS deposit_token_standard
, from_hex(json_extract_scalar(json_extract_scalar(receipt, '$.mainchain'), '$.tokenAddr')) AS deposit_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(json_extract_scalar(receipt, '$.id') AS varchar) AS bridge_transfer_id
FROM {{ source('axieinfinity_ethereum', 'mainchaingatewayv2_evt_depositrequested') }}