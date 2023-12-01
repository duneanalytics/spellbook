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
, txs.value/1e18/nftt.amount AS total_price
, pu.price*(txs.value/1e18/nftt.amount) AS total_price_usd
, nftt.to AS recipient
, nftt.tx_hash
, nftt.evt_index
, nftt.contract_address
, txs."from" AS tx_from
, txs.to AS tx_to
FROM {{ ref('nft_zora_transfers') }} nftt
INNER JOIN erc721_created_collections cc ON cc.contract_address=nftt.contract_address AND nftt.block_number>=cc.block_number
INNER JOIN {{ source('zora', 'transactions')}} txs ON txs.block_number=nftt.block_number
        AND txs.hash=nftt.tx_hash
        {% if is_incremental() %}
        AND {{incremental_predicate('txs.block_time')}}
        {% endif %}
INNER JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain='zora'
        AND pu.contract_address=0x4200000000000000000000000000000000000006
        AND pu.minute=date_trunc('minute', nftt.block_time)
        {% if is_incremental() %}
        AND {{incremental_predicate('pu.minute')}}
        {% endif %}
CROSS JOIN UNNEST(sequence(1, CAST(amount AS BIGINT))) AS t (sequence_element)
WHERE nftt."from"=0x0000000000000000000000000000000000000000
{% if is_incremental() %}
AND {{incremental_predicate('nftt.block_time')}}
{% endif %}

UNION ALL

SELECT 'zora' AS blockchain
, nftt.block_time
, nftt.block_number
, nftt.token_standard
, nftt.token_id
, nftt.amount AS quantity
, txs.value/1e18 AS total_price
, pu.price*(txs.value/1e18) AS total_price
, nftt.to AS recipient
, nftt.tx_hash
, nftt.evt_index
, nftt.contract_address
, txs."from" AS tx_from
, txs.to AS tx_to
FROM {{ ref('nft_zora_transfers') }} nftt
INNER JOIN erc1155_created_collections cc ON cc.contract_address=nftt.contract_address AND nftt.block_number>=cc.block_number
INNER JOIN {{ source('zora', 'transactions')}} txs ON txs.block_number=nftt.block_number
        AND txs.hash=nftt.tx_hash
        {% if is_incremental() %}
        AND {{incremental_predicate('txs.block_time')}}
        {% endif %}
INNER JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain='zora'
        AND pu.contract_address=0x4200000000000000000000000000000000000006
        AND pu.minute=date_trunc('minute', nftt.block_time)
        {% if is_incremental() %}
        AND {{incremental_predicate('pu.minute')}}
        {% endif %}
WHERE nftt."from"=0x0000000000000000000000000000000000000000
{% if is_incremental() %}
AND {{incremental_predicate('nftt.block_time')}}
{% endif %}