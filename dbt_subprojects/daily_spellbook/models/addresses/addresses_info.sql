{{ config(

        schema = 'addresses',
        alias ='info',
        materialized = 'table',
        unique_key = ['address'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "base", "celo", "scroll", "zora", "blast"]\',
                                    "sector",
                                    "addresses",
                                    \'["hildobby"]\') }}'
)
}}

{% set addresses_models = [
    ref('addresses_arbitrum_info')
    , ref('addresses_avalanche_c_info')
    , ref('addresses_base_info')
    , ref('addresses_blast_info')
    , ref('addresses_bnb_info')
    , ref('addresses_celo_info')
    , ref('addresses_ethereum_info')
    , ref('addresses_gnosis_info')
    , ref('addresses_optimism_info')
    , ref('addresses_polygon_info')
    , ref('addresses_scroll_info')
    , ref('addresses_zora_info')
] %}

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
    , map_from_entries(array_agg(struct(
        blockchain, 
        struct(last_seen, sent_count, received_count)
        ))) AS chain_stats
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
        SELECT blockchain
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
        FROM {{ addresses_model }}
        {% if is_incremental() %}
        WHERE {{incremental_predicate('creation_block_time')}}
        {% endif %}
        GROUP BY address
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
, chain_stats
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
