{{ config(
        schema = 'addresses',
        alias = 'info',
        unique_key = ['blockchain', 'address', 'dex_name', 'distinct_name'],
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "optimism", "polygon", "scroll", "zksync"]\',
                                "sector",
                                "dex",
                                \'["hildobby"]\') }}'
        )
}}


{% set address_models = [  
    ref('addresses_arbitrum_info')
    , ref('addresses_avalanche_c_info')
    , ref('addresses_base_info')
    , ref('addresses_bnb_info')
    , ref('addresses_celo_info')
    , ref('addresses_ethereum_info')
    , ref('addresses_optimism_info')
    , ref('addresses_polygon_info')
    , ref('addresses_scroll_info')
    , ref('addresses_zksync_info')
] %}


SELECT address
, array_agg(blockchain) AS blockchains
, COUNT(blockchain) AS number_of_chains_where_seen
, SUM(executed_tx_count) AS executed_tx_count
, MAX(max_nonce) AS max_nonce
, MAX(is_smart_contract) AS is_smart_contract
, MIN_BY(namespace, first_received_block_time) FILTER (WHERE namespace IS NOT NULL) AS namespace
, MIN_BY(name, first_received_block_time) FILTER (WHERE name IS NOT NULL) AS name
, MIN_BY(first_funded_by, first_received_block_time) AS first_funded_by
, MIN(first_tx_block_time) AS first_tx_block_time
, MAX(last_tx_block_time) AS last_tx_block_time
, MIN(first_tx_block_number) AS first_tx_block_number
, MAX(last_tx_block_number) AS last_tx_block_number
, MIN(first_received_block_time) AS first_received_block_time
, MIN(first_received_block_number) AS first_received_block_number
, MAX(last_transfer_block_time) AS last_transfer_block_time
, MAX(last_transfer_block_number) AS last_transfer_block_number
, MAX(last_seen) AS last_seen
FROM (
    {% for model in address_models %}
    SELECT
    *
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
GROUP BY 1

