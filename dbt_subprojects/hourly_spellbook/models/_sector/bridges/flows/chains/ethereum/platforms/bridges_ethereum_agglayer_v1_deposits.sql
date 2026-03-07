{% set blockchain = 'ethereum' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'agglayer_v1_deposits',
    materialized = 'view',
    )
}}

WITH bridge_events AS (
    SELECT d.destinationNetwork AS withdrawal_chain_id
    , evt_block_date AS block_date
    , evt_block_time AS block_time
    , evt_block_number AS block_number
    , amount AS deposit_amount_raw
    , CASE WHEN originAddress = 0x0000000000000000000000000000000000000000 THEN evt_tx_from ELSE originAddress END AS sender
    , destinationAddress AS recipient
    , CASE WHEN metadata=0x THEN 0x0000000000000000000000000000000000000000 ELSE NULL END AS deposit_token_address
    , CASE WHEN metadata=0x THEN 'native' ELSE 'erc20' END AS deposit_token_standard
    , evt_tx_from AS tx_from
    , evt_tx_hash AS tx_hash
    , evt_index
    , contract_address
    , CAST(depositCount AS varchar) AS bridge_transfer_id
    FROM {{ source('polygon_zkevm_ethereum', 'polygonzkevmbridge_evt_bridgeevent') }} d
    WHERE amount > 0
    )

, results AS (
    SELECT '{{blockchain}}' AS deposit_chain
    , be.withdrawal_chain_id
    , i.blockchain AS withdrawal_chain
    , 'Agglayer' AS bridge_name
    , '1' AS bridge_version
    , be.block_date
    , be.block_time
    , be.block_number
    , be.deposit_amount_raw
    , COALESCE(t."from", be.sender) AS sender
    , be.recipient
    , COALESCE(t.contract_address, be.deposit_token_address) AS deposit_token_address
    , be.deposit_token_standard
    , be.tx_from
    , be.tx_hash
    , be.evt_index
    , be.contract_address
    , be.bridge_transfer_id
    , ROW_NUMBER() OVER (PARTITION BY be.tx_hash, be.evt_index ORDER BY be.evt_index) AS duplicate_index
    FROM bridge_events be
    LEFT JOIN {{ source('tokens_ethereum', 'transfers') }} t ON t.block_number=be.block_number
        AND t.tx_hash=be.tx_hash
        AND t.to=be.contract_address
        AND t.amount_raw=be.deposit_amount_raw
        AND t.token_standard='erc20'
    LEFT JOIN {{ ref('bridges_agglayer_chain_indexes') }} i ON i.id=be.withdrawal_chain_id
    )

SELECT deposit_chain
, withdrawal_chain_id
, withdrawal_chain
, bridge_name
, bridge_version
, block_date
, block_time
, block_number
, deposit_amount_raw
, sender
, recipient
, deposit_token_address
, deposit_token_standard
, tx_from
, tx_hash
, evt_index
, contract_address
, bridge_transfer_id
FROM results
WHERE duplicate_index = 1