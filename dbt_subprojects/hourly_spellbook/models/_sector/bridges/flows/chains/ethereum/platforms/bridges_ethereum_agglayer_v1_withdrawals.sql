{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'agglayer_v1_withdrawals',
    materialized = 'view',
    )
}}

SELECT 'ronin' AS deposit_chain
, 2020 AS deposit_chain_id
, 'ethereum' AS withdrawal_chain
, 'Ronin' AS bridge_name
, '1' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, _tokenNumber AS withdrawal_amount_raw
, _owner AS sender
, _owner AS recipient
, CASE WHEN _tokenAddress =0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN 'native' ELSE 'erc20' END AS withdrawal_token_standard
, _tokenAddress AS withdrawal_token_address
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(_withdrawId AS varchar) AS bridge_transfer_id
FROM {{ source('axieinfinity_ethereum', 'mainchaingatewaymanager_evt_tokenwithdrew') }}