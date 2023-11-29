{{ config(
        schema = 'zora_zora',
        alias = 'mints',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

WITH erc721_created_collections AS (
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , editionContractAddress AS contract_address
    , creator
    , evt_index
    FROM {{ source('zora_zora', 'ZoraNFTCreatorV1_evt_CreatedDrop') }}
    )

, erc1155_created_collections AS (
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , newContract AS contract_address
    , creator
    , evt_index
    FROM {{ source('zora_zora', 'ZoraCreator1155_evt_SetupNewContract') }}
    )

SELECT 'zora' AS blockchain
, nftt.block_time
, nftt.block_number
, nftt.token_standard
, nftt.token_id
, 1 AS quantity
, value/1e18/amount AS total_price
, nftt.to AS recipient
, nftt.tx_hash
, nftt.evt_index
, nftt.contract_address
FROM {{ ref('nft_zora_transfers') }} nftt
INNER JOIN erc721_created_collections cc ON cc.contract_address=nftt.contract_address AND nftt.block_number>=cc.block_number
INNER JOIN {{ source('zora', 'transactions')}} txs ON txs.block_number=nftt.block_number
        AND txs.hash=nftt.tx_hash
CROSS JOIN UNNEST(sequence(1, CAST(amount AS BIGINT))) AS t (sequence_element)
WHERE nftt."from"=0x0000000000000000000000000000000000000000
{% if is_incremental() %}
AND nftt.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

UNION ALL

SELECT 'zora' AS blockchain
, nftt.block_time
, nftt.block_number
, nftt.token_standard
, nftt.token_id
, amount AS quantity
, value/1e18 AS total_price
, nftt.to AS recipient
, nftt.tx_hash
, nftt.evt_index
, nftt.contract_address
FROM {{ ref('nft_zora_transfers') }} nftt
INNER JOIN erc1155_created_collections cc ON cc.contract_address=nftt.contract_address AND nftt.block_number>=cc.block_number
INNER JOIN {{ source('zora', 'transactions')}} txs ON txs.block_number=nftt.block_number
        AND txs.hash=nftt.tx_hash
WHERE nftt."from"=0x0000000000000000000000000000000000000000
{% if is_incremental() %}
AND nftt.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}