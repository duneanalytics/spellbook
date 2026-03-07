{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'agglayer_v1_withdrawals',
    materialized = 'view',
    )
}}


WITH bridge_events AS (
    SELECT d.originNetwork AS deposit_chain_id
    , evt_block_date AS block_date
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , amount AS withdrawal_amount_raw
    , originAddress AS sender
    , destinationAddress AS recipient
    , evt_tx_from AS tx_from
    , evt_tx_hash AS tx_hash
    , evt_index
    , contract_address
    , CAST(index AS varchar) AS bridge_transfer_id
    FROM {{ source('polygon_zkevm_ethereum', 'polygonzkevmbridge_evt_claimevent') }} d
    WHERE amount > 0
    LIMIT 1000
    )

SELECT i.blockchain AS deposit_chain
, be.deposit_chain_id
, '{{blockchain}}' AS withdrawal_chain
, 'Agglayer' AS bridge_name
, '1' AS bridge_version
, be.block_date
, be.block_time
, be.block_number
, be.withdrawal_amount_raw
, COALESCE(t."from", be.sender) AS sender
, be.recipient
, COALESCE(t.contract_address, 0x0000000000000000000000000000000000000000) AS withdrawal_token_address
, CASE WHEN t.contract_address IS NOT NULL THEN 'erc20' ELSE 'native' END AS withdrawal_token_standard
, be.tx_from
, be.tx_hash
, be.evt_index
, be.contract_address
, be.bridge_transfer_id
FROM bridge_events be
LEFT JOIN {{ source('tokens_ethereum', 'transfers') }} t ON t.block_number=be.block_number
    AND t.tx_hash=be.tx_hash
    AND t."from"=be.contract_address
    AND t.amount_raw=be.withdrawal_amount_raw
    AND t.token_standard='erc20'
    AND t.block_time > NOW() - interval '4' day
LEFT JOIN {{ ref('bridges_agglayer_chain_indexes') }} i ON i.id=be.deposit_chain_id
