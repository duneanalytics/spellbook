{%- macro tesseract_icm_blockchains(
        blockchain = null
    )
-%}

{%- set namespace_blockchain = 'avalanche_teleporter_' + blockchain -%}

WITH combined_info AS (
    {% if is_incremental() -%}
    SELECT
        blockchain_id
        , earliest_icm_interaction
        , latest_icm_interaction
        , sample_message_id
    FROM {{ this }}
    {% else -%}
    SELECT
        blockchainID AS blockchain_id
        , MIN(evt_block_time) AS earliest_icm_interaction
        , NULL AS latest_icm_interaction
        , NULL AS sample_message_id
    FROM {{ source(namespace_blockchain, 'TeleporterMessenger_evt_BlockchainIDInitialized')}}
    GROUP BY blockchainID
    {% endif -%}
    UNION ALL
    SELECT
        destinationBlockchainID AS blockchain_id
        , MIN(evt_block_time) AS earliest_icm_interaction
        , MAX(evt_block_time) AS latest_icm_interaction
        , ANY_VALUE(messageID) AS sample_message_id
    FROM {{ source(namespace_blockchain, 'TeleporterMessenger_evt_SendCrossChainMessage') }}
    {% if is_incremental() -%}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif -%}
    GROUP BY destinationBlockchainID
    UNION ALL
    SELECT
        sourceBlockchainID AS blockchain_id
        , MIN(evt_block_time) AS earliest_icm_interaction
        , MAX(evt_block_time) AS latest_icm_interaction
        , ANY_VALUE(messageID) AS sample_message_id
    FROM {{ source(namespace_blockchain, 'TeleporterMessenger_evt_ReceiveCrossChainMessage') }}
    {% if is_incremental() -%}
    WHERE
        {{ incremental_predicate('evt_block_time') }}
    {% endif -%}
    GROUP BY sourceBlockchainID
)
SELECT
    '{{ blockchain }}' AS blockchain
    , blockchain_id
    , to_base58(
        varbinary_concat(
            blockchain_id,
            varbinary_substring(sha256(blockchain_id), 29, 4)
            )
        ) AS blockchain_id_base58
    , MIN(earliest_icm_interaction) AS earliest_icm_interaction
    , MAX(latest_icm_interaction) AS latest_icm_interaction
    , ANY_VALUE(sample_message_id) AS sample_message_id
FROM combined_info
GROUP BY blockchain_id

{%- endmacro -%}