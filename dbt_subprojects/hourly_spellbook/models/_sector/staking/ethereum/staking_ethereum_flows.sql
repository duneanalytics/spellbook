{{ config(
    
    schema = 'staking_ethereum',
    alias = 'flows',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby", "0xBoxer"]\') }}')
}}

WITH indexes AS (
    SELECT pubkey
    , i.validator_index AS validator_index
    , MAX(entity) AS entity
    , MAX(entity_category) AS entity_category
    , MAX(entity_unique_name) AS entity_unique_name
    , MAX(sub_entity) AS sub_entity
    , MAX(sub_entity_category) AS sub_entity_category
    , MAX(sub_entity_unique_name) AS sub_entity_unique_name
    FROM {{ ref('staking_ethereum_deposits')}}
    INNER JOIN {{source('dune', 'hildobby', 'dataset_ethereum_validators')}} i USING (pubkey)
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
    INNER JOIN {{source('dune', 'hildobby', 'dataset_ethereum_validators')}} i USING (pubkey)
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












{{ config(
    
    schema = 'staking_ethereum',
    alias = 'flows',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby", "0xBoxer"]\') }}')
}}

WITH deposits AS (
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
    INNER JOIN {{source('dune', 'hildobby', 'dataset_ethereum_validators')}} i USING (pubkey)
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
INNER JOIN {{source('dune', 'hildobby', 'dataset_ethereum_validators')}} i USING (validator_index)
