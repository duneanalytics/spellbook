{{ config(
        schema = 'addresses',
        alias ='info',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "base", "celo", "scroll", "zora", "blast", "fantom", "linea", "zkevm", "zksync"]\',
                                    "sector",
                                    "addresses",
                                    \'["hildobby"]\') }}'
)
}}

{% set addresses_models = [
    ('arbitrum', ref('addresses_arbitrum_info'))
    , ('avalanche_c', ref('addresses_avalanche_c_info'))
    , ('base', ref('addresses_base_info'))
    , ('blast', ref('addresses_blast_info'))
    , ('bnb', ref('addresses_bnb_info'))
    , ('celo', ref('addresses_celo_info'))
    , ('ethereum', ref('addresses_ethereum_info'))
    , ('gnosis', ref('addresses_gnosis_info'))
    , ('optimism', ref('addresses_optimism_info'))
    , ('polygon', ref('addresses_polygon_info'))
    , ('scroll', ref('addresses_scroll_info'))
    , ('zora', ref('addresses_zora_info'))
    , ('fantom', ref('addresses_fantom_info'))
    , ('linea', ref('addresses_linea_info'))
    , ('zkevm', ref('addresses_zkevm_info'))
    , ('zksync', ref('addresses_zksync_info'))
] %}

{% if not is_incremental() %}

WITH data AS (
    SELECT address
    , array_agg(blockchain) AS blockchains
    , SUM(executed_tx_count) AS executed_tx_count
    , MAX(max_nonce) AS max_nonce
    , MAX_BY(blockchain,max_nonce) AS max_nonce_blockchain
    , MAX(is_smart_contract) AS is_smart_contract
    , array_agg(blockchain) FILTER (WHERE is_smart_contract) AS smart_contract_blockchains
    , MAX(namespace) AS namespace
    , MAX(name) AS name
    , MIN_BY(first_funded_by, first_funded_by_block_time) AS first_funded_by
    , MIN_BY(blockchain, first_funded_by_block_time) AS first_funded_blockchain
    , MIN(first_funded_by_block_time) AS first_funded_by_block_time
    , SUM(tokens_received_count) AS tokens_received_count
    , SUM(tokens_received_tx_count) AS tokens_received_tx_count
    , SUM(tokens_sent_count) AS tokens_sent_count
    , SUM(tokens_sent_tx_count) AS tokens_sent_tx_count
    , MIN(first_transfer_block_time) AS first_transfer_block_time
    , MAX(last_transfer_block_time) AS last_transfer_block_time
    , MIN(first_received_block_number) AS first_received_block_number
    , MAX(last_received_block_number) AS last_received_block_number
    , MIN(first_sent_block_number) AS first_sent_block_number
    , MAX(last_sent_block_number) AS last_sent_block_number
    , SUM(sent_volume_usd) AS sent_volume_usd
    , SUM(received_volume_usd) AS received_volume_usd
    , MIN(first_tx_block_time) AS first_tx_block_time
    , MAX(last_tx_block_time) AS last_tx_block_time
    , map_union(map_from_entries(array[
            (blockchain, chain_stats)
            ])) AS chain_stats
    , MAX(last_seen) AS last_seen
    , MAX(last_seen_block) AS last_seen_block
    FROM (
        {% for addresses_model in addresses_models %}
        SELECT '{{ addresses_model[0] }}' AS blockchain
        , address
        , executed_tx_count
        , max_nonce
        , is_smart_contract
        , namespace
        , name
        , first_funded_by
        , first_funded_by_block_time
        , tokens_received_count
        , tokens_received_tx_count
        , tokens_sent_count
        , tokens_sent_tx_count
        , first_transfer_block_time
        , last_transfer_block_time
        , first_received_block_number
        , last_received_block_number
        , first_sent_block_number
        , last_sent_block_number
        , received_volume_usd
        , sent_volume_usd
        , first_tx_block_time
        , last_tx_block_time
        , first_tx_block_number
        , last_tx_block_number
        , map_from_entries(array[
            ('last_seen', CAST(last_seen AS varchar))
            , ('last_seen_block', CAST(last_seen_block AS varchar))
            , ('executed_tx_count', CAST(executed_tx_count AS varchar))
            , ('is_smart_contract', CAST(is_smart_contract AS varchar))
            , ('tokens_sent_count', CAST(tokens_sent_count AS varchar))
            , ('tokens_received_count', CAST(tokens_received_count AS varchar))
            ]) AS chain_stats
        , last_seen
        , last_seen_block
        FROM (select * from {{ addresses_model[1] }}
        ORDER BY address asc limit 2000000)
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
    GROUP BY address
    )

SELECT address
, blockchains
, executed_tx_count
, max_nonce
, max_nonce_blockchain
, is_smart_contract
, smart_contract_blockchains
, namespace
, name
, first_funded_by
, first_funded_blockchain
, first_funded_by_block_time
, tokens_received_count
, tokens_received_tx_count
, tokens_sent_count
, tokens_sent_tx_count
, first_transfer_block_time
, last_transfer_block_time
, first_received_block_number
, last_received_block_number
, first_sent_block_number
, last_sent_block_number
, sent_volume_usd
, received_volume_usd
, first_tx_block_time
, last_tx_block_time
, chain_stats
, last_seen
, last_seen_block
FROM data



{% else %}



WITH to_update AS (
    SELECT DISTINCT address
    FROM (
        {% for addresses_model in addresses_models %}
        SELECT am.address
        FROM {{ addresses_model[1] }} am
        LEFT JOIN {{ this }} t ON am.address = t.address
        WHERE (t.address IS NULL
            OR ((contains(t.blockchains, am.blockchain) = FALSE))
            OR (CAST(t.chain_stats['{{ addresses_model[0] }}']['last_seen_block'] AS bigint) <= am.last_seen_block))
        AND {{incremental_predicate('am.last_seen')}}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
    LIMIT 2000000
    )


SELECT address
, array_agg(blockchain) AS blockchains
, SUM(executed_tx_count) AS executed_tx_count
, MAX(max_nonce) AS max_nonce
, MAX_BY(blockchain,max_nonce) AS max_nonce_blockchain
, MAX(is_smart_contract) AS is_smart_contract
, array_agg(blockchain) FILTER (WHERE is_smart_contract) AS smart_contract_blockchains
, MAX(namespace) AS namespace
, MAX(name) AS name
, MIN_BY(first_funded_by, first_funded_by_block_time) AS first_funded_by
, MIN_BY(blockchain, first_funded_by_block_time) AS first_funded_blockchain
, MIN(first_funded_by_block_time) AS first_funded_by_block_time
, SUM(tokens_received_count) AS tokens_received_count
, SUM(tokens_received_tx_count) AS tokens_received_tx_count
, SUM(tokens_sent_count) AS tokens_sent_count
, SUM(tokens_sent_tx_count) AS tokens_sent_tx_count
, MIN(first_transfer_block_time) AS first_transfer_block_time
, MAX(last_transfer_block_time) AS last_transfer_block_time
, MIN(first_received_block_number) AS first_received_block_number
, MAX(last_received_block_number) AS last_received_block_number
, MIN(first_sent_block_number) AS first_sent_block_number
, MAX(last_sent_block_number) AS last_sent_block_number
, SUM(sent_volume_usd) AS sent_volume_usd
, SUM(received_volume_usd) AS received_volume_usd
, MIN(first_tx_block_time) AS first_tx_block_time
, MAX(last_tx_block_time) AS last_tx_block_time
, map_union(map_from_entries(array[
        (blockchain, chain_stats)
        ])) AS chain_stats
, MAX(last_seen) AS last_seen
, MAX(last_seen_block) AS last_seen_block
FROM (
    {% for addresses_model in addresses_models %}
    SELECT '{{ addresses_model[0] }}' AS blockchain
    , address
    , executed_tx_count
    , max_nonce
    , is_smart_contract
    , namespace
    , name
    , first_funded_by
    , first_funded_by_block_time
    , tokens_received_count
    , tokens_received_tx_count
    , tokens_sent_count
    , tokens_sent_tx_count
    , first_transfer_block_time
    , last_transfer_block_time
    , first_received_block_number
    , last_received_block_number
    , first_sent_block_number
    , last_sent_block_number
    , received_volume_usd
    , sent_volume_usd
    , first_tx_block_time
    , last_tx_block_time
    , first_tx_block_number
    , last_tx_block_number
    , map_from_entries(array[
        ('last_seen', CAST(last_seen AS varchar))
        , ('last_seen_block', CAST(last_seen_block AS varchar))
        , ('executed_tx_count', CAST(executed_tx_count AS varchar))
        , ('is_smart_contract', CAST(is_smart_contract AS varchar))
        , ('tokens_sent_count', CAST(tokens_sent_count AS varchar))
        , ('tokens_received_count', CAST(tokens_received_count AS varchar))
        ]) AS chain_stats
    , last_seen
    , last_seen_block
    FROM {{ addresses_model[1] }}
    INNER JOIN to_update USING (address)
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )
GROUP BY address

{% endif %}