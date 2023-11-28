{{ config(
        schema = 'zora_zora',
        alias = 'mints',
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['tx_hash', 'token_id', 'evt_index']
)
}}

WITH created_collections AS (
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , editionContractAddress AS contract_address
    , creator
    , evt_index
    FROM {{ source('zora_zora', 'ZoraNFTCreatorV1_evt_CreatedDrop') }}
    UNION
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
, NULL AS token_price
, nftt.to AS recipient
, nftt.tx_hash
, nftt.evt_index
, nftt.contract_address
FROM {{ ref('nft_zora_transfers') }} nftt
INNER JOIN created_collections cc ON cc.contract_address=nftt.contract_address AND nftt.block_number>=cc.block_number
WHERE nftt."from"=0x0000000000000000000000000000000000000000
{% if is_incremental() %}
AND nftt.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
