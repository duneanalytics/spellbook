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
        , (0xb913a27913c8a74c05cc31c1a690ecad05a59e405bd7c5e8f6eab8e426e041a98b26cf40b04356b4d92ac20a56b7dcae)
        , (0x86af099d9134b2994b835cced1fadfb4587dddfc4010470db9d8875cffd9e5a62a197db7d9c0266fb67c5175feb7ef51)
        , (0x8107543db1d5c69be127b3eb84c0f7b8157b892482f7e98b85b83d9a6be75e7c24a645df6283d46b16285862530cad77)
        , (0x8e05d99c557001d06f8240d46899b829a8cc77ac57b3d16359279cad707cfe5f223a3374987ab73a379ee358dd05d524)
        , (0xaebf3fbab24f55df829e2bc939bb987cbdb3edea7d4cb8877e422f1185d03a22f3a0f6449a1d6d1b912ec09ed11f2bb3)
        , (0xac3a0887866d5d45555904e2cb35e1b89b4c338c19001b0cc1184c9f95c5a731ccef70dc4c4fed7709c2106042a119c9)
        , (0xa03840dd6af6555442e3fc0d62de8dde77970f45175ea9926327372b5c83542f67cdd06e14e1daa44a3ae23e4d8eef52)
        , (0x828116d0d2e945f1483ec7c6c135a8e00814588879e9a12a67c42268c339388b6f796f6c858e673f6000f5d028b913da)
        , (0x816827749a5194cf8389419e88d87f6786436daaa5546b92c68af015f0b7e17c66f4bb30b18872f3f051c2bc213ecaab)
        , (0xa0ab932b24d80a7a96f0fb32ce2aa724625eb090c70cb9c977f0bc5909503629155335be021e58b245e11a77c847a11c)
        , (0x8d135f9185f635be5e3d738c835a02d5efab05bc5fa38ce4cb6d02156446aa3cc1ce9cc38576ec519af815dc39d52c81)
        , (0x8151e62f956cf1562007d9620fd4e91c029fb43959d1a7d1d2168c2943d65a3ef31764d1cc2d2540fb26ce86efad2ffd)
        , (0x9714e943c81d802f3c858f284fff25779818a903c034a3de42da7a2b63ae6632c52b2be0982007e8090d0d334f8cf656)
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
