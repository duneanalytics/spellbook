{{ config(

        schema = 'addresses',
        alias ='info',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['address'],
        merge_update_columns = ['blockchains', 'executed_tx_count', 'max_nonce', 'max_nonce_blockchain', 'is_smart_contract', 'smart_contract_blockchains', 'namespace', 'name', 'first_funded_by', 'first_funded_blockchain', 'first_funded_by_block_time', 'sent_count', 'received_count', 'first_received_block_time', 'last_received_block_time', 'first_sent_block_time', 'last_sent_block_time', 'sent_volume_usd', 'received_volume_usd', 'first_tx_block_time', 'last_tx_block_time', 'last_seen'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "base", "celo", "scroll", "zora", "blast"]\',
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
    , SUM(sent_count) AS sent_count
    , SUM(received_count) AS received_count
    , MIN(first_received_block_time) AS first_received_block_time
    , MAX(last_received_block_time) AS last_received_block_time
    , MIN(first_sent_block_time) AS first_sent_block_time
    , MAX(last_sent_block_time) AS last_sent_block_time
    , SUM(sent_volume_usd) AS sent_volume_usd
    , SUM(received_volume_usd) AS received_volume_usd
    , MIN(first_tx_block_time) AS first_tx_block_time
    , MAX(last_tx_block_time) AS last_tx_block_time
    , MAX(last_seen) AS last_seen
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
        , received_count
        , sent_count
        , first_received_block_time
        , last_received_block_time
        , first_sent_block_time
        , last_sent_block_time
        , received_volume_usd
        , sent_volume_usd
        , first_tx_block_time
        , last_tx_block_time
        , first_tx_block_number
        , last_tx_block_number
        , last_seen
        FROM {{ addresses_model[1] }}
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
, sent_count
, received_count
, first_received_block_time
, last_received_block_time
, first_sent_block_time
, last_sent_block_time
, sent_volume_usd
, received_volume_usd
, first_tx_block_time
, last_tx_block_time
, last_seen
FROM data



{% else %}



WITH new_data AS (
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
    , SUM(sent_count) AS sent_count
    , SUM(received_count) AS received_count
    , MIN(first_received_block_time) AS first_received_block_time
    , MAX(last_received_block_time) AS last_received_block_time
    , MIN(first_sent_block_time) AS first_sent_block_time
    , MAX(last_sent_block_time) AS last_sent_block_time
    , SUM(sent_volume_usd) AS sent_volume_usd
    , SUM(received_volume_usd) AS received_volume_usd
    , MIN(first_tx_block_time) AS first_tx_block_time
    , MAX(last_tx_block_time) AS last_tx_block_time
    , MAX(last_seen) AS last_seen
    FROM (
        {% for addresses_model in addresses_models %}
        SELECT '{{ addresses_model[0] }}' AS blockchain
        , am.address
        , am.executed_tx_count
        , am.max_nonce
        , am.is_smart_contract
        , am.namespace
        , am.name
        , am.first_funded_by
        , am.first_funded_by_block_time
        , am.received_count
        , am.sent_count
        , am.first_received_block_time
        , am.last_received_block_time
        , am.first_sent_block_time
        , am.last_sent_block_time
        , am.received_volume_usd
        , am.sent_volume_usd
        , am.first_tx_block_time
        , am.last_tx_block_time
        , am.first_tx_block_number
        , am.last_tx_block_number
        , am.last_seen
        FROM {{ addresses_model[1] }} am
        LEFT JOIN {{ this }} t ON am.address = t.address
            AND (am.last_seen > t.last_seen OR contains(t.blockchains, am.blockchain) = FALSE)
        WHERE {{incremental_predicate('am.last_seen')}}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
    GROUP BY address
    )

SELECT nd.address
, array_union(t.blockchains, array_agg(nd.blockchain)) AS blockchains
, t.executed_tx_count+nd.executed_tx_count AS executed_tx_count
, GREATEST(t.max_nonce, nd.max_nonce) AS max_nonce
, CASE WHEN GREATEST(t.max_nonce, nd.max_nonce) = t.max_nonce THEN t.max_nonce_blockchain
    ELSE nd.max_nonce_blockchain
    END AS max_nonce_blockchain
, GREATEST(t.is_smart_contract, nd.is_smart_contract) AS is_smart_contract
, array_union(t.smart_contract_blockchains, nd.smart_contract_blockchains) AS smart_contract_blockchains
, COALESCE(nd.namespace, t.namespace) AS namespace
, COALESCE(nd.name, t.name) AS name
, CASE WHEN LEAST(t.first_funded_by_block_time, nd.first_funded_by_block_time) = t.first_funded_by_block_time THEN t.first_funded_by
    ELSE nd.first_funded_by END AS first_funded_by
, CASE WHEN LEAST(t.first_funded_by_block_time, nd.first_funded_by_block_time) = t.first_funded_by_block_time THEN t.first_funded_blockchain
    ELSE nd.first_funded_blockchain END AS first_funded_blockchain
, LEAST(t.first_funded_by_block_time, nd.first_funded_by_block_time) AS first_funded_by_block_time
, t.sent_count+nd.sent_count AS sent_count
, t.received_count+nd.received_count AS received_count
, LEAST(t.first_received_block_time, nd.first_received_block_time) AS first_received_block_time
, GREATEST(t.last_received_block_time, nd.last_received_block_time) AS last_received_block_time
, LEAST(t.first_sent_block_time, nd.first_sent_block_time) AS first_sent_block_time
, GREATEST(t.last_sent_block_time, nd.last_sent_block_time) AS last_sent_block_time
, t.sent_volume_usd+nd.sent_volume_usd AS sent_volume_usd
, t.received_volume_usd+nd.received_volume_usd AS received_volume_usd
, LEAST(t.first_tx_block_time, nd.first_tx_block_time) AS first_tx_block_time
, GREATEST(t.last_tx_block_time, nd.last_tx_block_time) AS last_tx_block_time
, GREATEST(t.last_seen, nd.last_seen) AS last_seen
FROM new_data nd
LEFT JOIN {{ this }} t ON nd.address = t.address

{% endif %}