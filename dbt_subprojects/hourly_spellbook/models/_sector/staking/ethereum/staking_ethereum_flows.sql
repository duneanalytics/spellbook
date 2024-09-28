{{ config(
    
    schema = 'staking_ethereum',
    alias = 'flows',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby", "0xBoxer"]\') }}')
}}

WITH invalid_pubkeys AS (
    SELECT pubkey
    FROM (VALUES
        (0xb01dd8e44a8e02e36e0d66161103b9ff32315dbb9ae8c8ac8d097ba86a9e2b1eb3c7fd41e7cd1f77a987985639c26f52)
        , (0xac424d8a3e6ce38eb22109125357324a1c44ecad7a330a3d3deff91e68f4b567ba38c065d2cf852ef050d21705e5dfcb)
        , (0x918f080ca717afed4966901794ad8222ca618b523bbd3ce94be4a1240aa69d9be20f884950214a3cafa0404ce41213e1)
        , (0xa8bcbf91bff7d3368ddbf5b35c46a4f5d82b16230c851a4b8eec82be45225d339414170e14a6cd17ad83ee3792dead85)
        , (0x86f473a006c566f1648a82c74cdfbd4a3cb2ea04eb2e0d49ef381ab2562576888554ef3d39e56996f24c804abb489600)
        , (0x8c69edd7a8e8da5330787952a1ad5075516e6fd4bda1586d62dd64701f7628d5229eb7f929017dea9ae6995f9c69ef5e)
        , (0x80a29e569e8ced0be1fff42c845a59449aecf8a2503542e4e76763ccc0265e683e2d5d46618cc829349293ed08ff49ff)
        --, (0x00) -- This is a dummy pubkey used to refresh the table
        ) AS temp_table (pubkey)
    )

, indexes AS (
    SELECT pubkey
    , ROW_NUMBER() OVER (ORDER BY MIN(deposit_index)) - 1 AS validator_index
    , MAX(entity) AS entity
    , MAX(entity_category) AS entity_category
    , MAX(entity_unique_name) AS entity_unique_name
    , MAX(sub_entity) AS sub_entity
    , MAX(sub_entity_category) AS sub_entity_category
    , MAX(sub_entity_unique_name) AS sub_entity_unique_name
    FROM {{ ref('staking_ethereum_deposits')}}
    WHERE pubkey NOT IN (SELECT pubkey FROM invalid_pubkeys)
    GROUP BY 1
    )

, deposits AS (
    SELECT d.block_time
    , d.block_number
    , d.amount_staked
    , d.depositor_address
    , d.entity
    , d.entity_unique_name
    , d.entity_category
    , d.sub_entity
    , d.sub_entity_unique_name
    , d.sub_entity_category
    , d.tx_hash
    , d.tx_from
    , d.deposit_index
    , i.validator_index
    , pubkey
    , d.signature
    , d.withdrawal_address
    , d.withdrawal_credentials
    , d.withdrawal_credentials_type
    , d.evt_index
    FROM {{ ref('staking_ethereum_deposits')}} d
    INNER JOIN indexes i USING (pubkey)
    )
    
SELECT block_time
, block_number
, amount_staked
, depositor_address
, entity
, entity_unique_name
, entity_category
, sub_entity
, sub_entity_unique_name
, sub_entity_category
, tx_from
, deposit_index
, NULL AS withdrawal_index
, validator_index
, pubkey
, signature
, withdrawal_address
, withdrawal_credentials_type
, withdrawal_credentials
, 0 AS amount_full_withdrawn
, 0 AS amount_partial_withdrawn
, tx_hash
, evt_index
FROM deposits

UNION ALL

SELECT w.block_time
, w.block_number
, 0 AS amount_staked
, NULL AS depositor_address
, i.entity
, i.entity_unique_name
, i.entity_category
, i.sub_entity
, i.sub_entity_unique_name
, i.sub_entity_category
, NULL AS tx_from
, NULL deposit_index
, w."index" AS withdrawal_index
, validator_index
, i.pubkey
, NULL AS signature
, w."address" AS withdrawal_address
, NULL AS withdrawal_credentials_type
, NULL AS withdrawal_credentials
, CASE WHEN w.amount/1e9 BETWEEN 20 AND 32 THEN w.amount/1e9 WHEN w.amount/1e9 > 32 THEN 32 END AS amount_full_withdrawn
, CASE WHEN w.amount/1e9 < 20 THEN w.amount/1e9 WHEN w.amount/1e9 > 32 THEN (w.amount/1e9)-32 END AS amount_partial_withdrawn
, NULL AS tx_hash
, NULL AS evt_index
FROM {{source('ethereum', 'withdrawals')}} w
INNER JOIN indexes i USING (validator_index)
