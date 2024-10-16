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
--
--{% if not is_incremental() %}
--
--WITH data AS (
--    SELECT address
--    , array_agg(blockchain) AS blockchains
--    , SUM(executed_tx_count) AS executed_tx_count
--    , MAX(max_nonce) AS max_nonce
--    , MAX_BY(blockchain,max_nonce) AS max_nonce_blockchain
--    , MAX(is_smart_contract) AS is_smart_contract
--    , array_agg(blockchain) FILTER (WHERE is_smart_contract) AS smart_contract_blockchains
--    , MAX(namespace) AS namespace
--    , MAX(name) AS name
--    , MIN_BY(first_funded_by, first_funded_by_block_time) AS first_funded_by
--    , MIN_BY(blockchain, first_funded_by_block_time) AS first_funded_blockchain
--    , MIN(first_funded_by_block_time) AS first_funded_by_block_time
--    , SUM(tokens_received_count) AS tokens_received_count
--    , SUM(tokens_received_tx_count) AS tokens_received_tx_count
--    , SUM(tokens_sent_count) AS tokens_sent_count
--    , SUM(tokens_sent_tx_count) AS tokens_sent_tx_count
--    , MIN(first_transfer_block_time) AS first_transfer_block_time
--    , MAX(last_transfer_block_time) AS last_transfer_block_time
--    , MIN(first_received_block_number) AS first_received_block_number
--    , MAX(last_received_block_number) AS last_received_block_number
--    , MIN(first_sent_block_number) AS first_sent_block_number
--    , MAX(last_sent_block_number) AS last_sent_block_number
--    , SUM(sent_volume_usd) AS sent_volume_usd
--    , SUM(received_volume_usd) AS received_volume_usd
--    , MIN(first_tx_block_time) AS first_tx_block_time
--    , MAX(last_tx_block_time) AS last_tx_block_time
--    , map_union(map_from_entries(array[
--            (blockchain, chain_stats)
--            ])) AS chain_stats
--    , MAX(last_seen) AS last_seen
--    , MAX(last_seen_block) AS last_seen_block
--    FROM (
--        {% for addresses_model in addresses_models %}
--        SELECT '{{ addresses_model[0] }}' AS blockchain
--        , address
--        , executed_tx_count
--        , max_nonce
--        , is_smart_contract
--        , namespace
--        , name
--        , first_funded_by
--        , first_funded_by_block_time
--        , tokens_received_count
--        , tokens_received_tx_count
--        , tokens_sent_count
--        , tokens_sent_tx_count
--        , first_transfer_block_time
--        , last_transfer_block_time
--        , first_received_block_number
--        , last_received_block_number
--        , first_sent_block_number
--        , last_sent_block_number
--        , received_volume_usd
--        , sent_volume_usd
--        , first_tx_block_time
--        , last_tx_block_time
--        , first_tx_block_number
--        , last_tx_block_number
--        , map_from_entries(array[
--            ('last_seen', CAST(last_seen AS varchar))
--            , ('last_seen_block', CAST(last_seen_block AS varchar))
--            , ('executed_tx_count', CAST(executed_tx_count AS varchar))
--            , ('is_smart_contract', CAST(is_smart_contract AS varchar))
--            , ('sent_count', CAST(sent_count AS varchar))
--            , ('received_count', CAST(received_count AS varchar))
--            ]) AS chain_stats
--        , last_seen
--        , last_seen_block
--        FROM {{ addresses_model[1] }}
--        {% if not loop.last %}
--        UNION ALL
--        {% endif %}
--        {% endfor %}
--        )
--    GROUP BY address
--    )
--
--SELECT address
--, blockchains
--, executed_tx_count
--, max_nonce
--, max_nonce_blockchain
--, is_smart_contract
--, smart_contract_blockchains
--, namespace
--, name
--, first_funded_by
--, first_funded_blockchain
--, first_funded_by_block_time
--, tokens_received_count
--, tokens_received_tx_count
--, tokens_sent_count
--, tokens_sent_tx_count
--, first_transfer_block_time
--, last_transfer_block_time
--, first_received_block_number
--, last_received_block_number
--, first_sent_block_number
--, last_sent_block_number
--, sent_volume_usd
--, received_volume_usd
--, first_tx_block_time
--, last_tx_block_time
--, chain_stats
--, last_seen
--, last_seen_block
--FROM data
--
--
--
--{% else %}
--
--
--
--WITH new_data AS (
--    SELECT address
--    , array_agg(blockchain) AS blockchains
--    , SUM(executed_tx_count) AS executed_tx_count
--    , MAX(max_nonce) AS max_nonce
--    , MAX_BY(blockchain,max_nonce) AS max_nonce_blockchain
--    , MAX(is_smart_contract) AS is_smart_contract
--    , array_agg(blockchain) FILTER (WHERE is_smart_contract) AS smart_contract_blockchains
--    , MAX(namespace) AS namespace
--    , MAX(name) AS name
--    , MIN_BY(first_funded_by, first_funded_by_block_time) AS first_funded_by
--    , MIN_BY(blockchain, first_funded_by_block_time) AS first_funded_blockchain
--    , MIN(first_funded_by_block_time) AS first_funded_by_block_time
--    , SUM(tokens_received_count) AS tokens_received_count
--    , SUM(tokens_received_tx_count) AS tokens_received_tx_count
--    , SUM(tokens_sent_count) AS tokens_sent_count
--    , SUM(tokens_sent_tx_count) AS tokens_sent_tx_count
--    , MIN(first_transfer_block_time) AS first_transfer_block_time
--    , MAX(last_transfer_block_time) AS last_transfer_block_time
--    , MIN(first_received_block_number) AS first_received_block_number
--    , MAX(last_received_block_number) AS last_received_block_number
--    , MIN(first_sent_block_number) AS first_sent_block_number
--    , MAX(last_sent_block_number) AS last_sent_block_number
--    , SUM(sent_volume_usd) AS sent_volume_usd
--    , SUM(received_volume_usd) AS received_volume_usd
--    , MIN(first_tx_block_time) AS first_tx_block_time
--    , MAX(last_tx_block_time) AS last_tx_block_time
--    , map_union(map_from_entries(array[
--            (blockchain, chain_stats)
--            ])) AS chain_stats
--    , MAX(last_seen) AS last_seen
--    , MAX(last_seen_block) AS last_seen_block
--    FROM (
--        {% for addresses_model in addresses_models %}
--        SELECT '{{ addresses_model[0] }}' AS blockchain
--        , am.address
--        , am.executed_tx_count
--        , am.max_nonce
--        , am.is_smart_contract
--        , am.namespace
--        , am.name
--        , am.first_funded_by
--        , am.first_funded_by_block_time
--        , am.tokens_received_count
--        , am.tokens_received_tx_count
--        , am.tokens_sent_count
--        , am.tokens_sent_tx_count
--        , am.first_transfer_block_time
--        , am.last_transfer_block_time
--        , am.first_received_block_number
--        , am.last_received_block_number
--        , am.first_sent_block_number
--        , am.last_sent_block_number
--        , am.received_volume_usd
--        , am.sent_volume_usd
--        , am.first_tx_block_time
--        , am.last_tx_block_time
--        , am.first_tx_block_number
--        , am.last_tx_block_number
--        , map_from_entries(array[
--            ('last_seen', CAST(am.last_seen AS varchar))
--            , ('last_seen_block', CAST(am.last_seen_block AS varchar))
--            , ('executed_tx_count', CAST(am.executed_tx_count AS varchar))
--            , ('is_smart_contract', CAST(am.is_smart_contract AS varchar))
--            , ('sent_count', CAST(am.sent_count AS varchar))
--            , ('received_count', CAST(am.received_count AS varchar))
--            ]) AS chain_stats
--        , am.last_seen
--        , am.last_seen_block
--        FROM {{ addresses_model[1] }} am
--        LEFT JOIN {{ this }} t ON am.address = t.address
--            AND (((contains(t.blockchains, am.blockchain) = FALSE))
--            OR (CAST(chain_stats['{{ addresses_model[0] }}']['last_seen_block'] AS bigint) > t.last_seen_block))
--        WHERE {{incremental_predicate('am.last_seen')}}
--        {% if not loop.last %}
--        UNION ALL
--        {% endif %}
--        {% endfor %}
--        )
--    GROUP BY address
--    )
--
--SELECT nd.address
--, array_union(t.blockchains, nd.blockchains) AS blockchains
--, t.executed_tx_count+nd.executed_tx_count AS executed_tx_count
--, GREATEST(t.max_nonce, nd.max_nonce) AS max_nonce
--, CASE WHEN GREATEST(t.max_nonce, nd.max_nonce) = t.max_nonce THEN t.max_nonce_blockchain
--    ELSE nd.max_nonce_blockchain
--    END AS max_nonce_blockchain
--, GREATEST(t.is_smart_contract, nd.is_smart_contract) AS is_smart_contract
--, array_union(t.smart_contract_blockchains, nd.smart_contract_blockchains) AS smart_contract_blockchains
--, COALESCE(nd.namespace, t.namespace) AS namespace
--, COALESCE(nd.name, t.name) AS name
--, CASE WHEN LEAST(t.first_funded_by_block_time, nd.first_funded_by_block_time) = t.first_funded_by_block_time THEN t.first_funded_by
--    ELSE nd.first_funded_by END AS first_funded_by
--, CASE WHEN LEAST(t.first_funded_by_block_time, nd.first_funded_by_block_time) = t.first_funded_by_block_time THEN t.first_funded_blockchain
--    ELSE nd.first_funded_blockchain END AS first_funded_blockchain
--, LEAST(t.first_funded_by_block_time, nd.first_funded_by_block_time) AS first_funded_by_block_time
--, t.tokens_received_count+nd.tokens_received_count AS tokens_received_count
--, t.tokens_received_tx_count+nd.tokens_received_tx_count AS tokens_received_tx_count
--, t.tokens_sent_count+nd.tokens_sent_count AS tokens_sent_count
--, t.tokens_sent_tx_count+nd.tokens_sent_tx_count AS tokens_sent_tx_count
--, LEAST(t.first_transfer_block_time, nd.first_transfer_block_time) AS first_transfer_block_time
--, GREATEST(t.last_transfer_block_time, nd.last_transfer_block_time) AS last_transfer_block_time
--, LEAST(t.first_received_block_number, nd.first_received_block_number) AS first_received_block_number
--, GREATEST(t.last_received_block_number, nd.last_received_block_number) AS last_received_block_number
--, LEAST(t.first_sent_block_number, nd.first_sent_block_number) AS first_sent_block_number
--, GREATEST(t.last_sent_block_number, nd.last_sent_block_number) AS last_sent_block_number
--, t.sent_volume_usd+nd.sent_volume_usd AS sent_volume_usd
--, t.received_volume_usd+nd.received_volume_usd AS received_volume_usd
--, LEAST(t.first_tx_block_time, nd.first_tx_block_time) AS first_tx_block_time
--, GREATEST(t.last_tx_block_time, nd.last_tx_block_time) AS last_tx_block_time
--, GREATEST(t.last_seen, nd.last_seen) AS last_seen
--, GREATEST(t.last_seen_block, nd.last_seen_block) AS last_seen_block
--, map_concat(map_filter(t.chain_stats, (k, v) -> NOT contains(map_keys(nd.chain_stats), k)), nd.chain_stats) AS chain_stats
--FROM new_data nd
--LEFT JOIN {{ this }} t ON nd.address = t.address
--
--{% endif %}
