{{ config
(
    alias = 'first_token_received',
    schema = 'addresses_events',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis", "optimism", "polygon", "celo", "zora", "base", "scroll", "zksync", "sei", "mantle", "blast", "ronin", "nova", "abstract", "berachain", "katana"]\',
                                    "sector",
                                    "addresses_events",
                                    \'["hildobby"]\') }}'
)
}}

{% set addresses_events_models = [
(ref('addresses_events_arbitrum_first_token_received'))
, (ref('addresses_events_avalanche_c_first_token_received'))
, (ref('addresses_events_bnb_first_token_received'))
, (ref('addresses_events_ethereum_first_token_received'))
, (ref('addresses_events_fantom_first_token_received'))
, (ref('addresses_events_gnosis_first_token_received'))
, (ref('addresses_events_optimism_first_token_received'))
, (ref('addresses_events_polygon_first_token_received'))
, (ref('addresses_events_celo_first_token_received'))
, (ref('addresses_events_zora_first_token_received'))
, (ref('addresses_events_base_first_token_received'))
, (ref('addresses_events_scroll_first_token_received'))
, (ref('addresses_events_zkevm_first_token_received'))
, (ref('addresses_events_linea_first_token_received'))
, (ref('addresses_events_sei_first_token_received'))
, (ref('addresses_events_mantle_first_token_received'))
, (ref('addresses_events_zksync_first_token_received'))
, (ref('addresses_events_blast_first_token_received'))
, (ref('addresses_events_ronin_first_token_received'))
, (ref('addresses_events_nova_first_token_received'))
, (ref('addresses_events_abstract_first_token_received'))
, (ref('addresses_events_apechain_first_token_received'))
, (ref('addresses_events_berachain_first_token_received'))
, (ref('addresses_events_katana_first_token_received'))
] %}

WITH joined_data AS (
    SELECT *
    FROM (
        {% for addresses_events_model in addresses_events_models %}
        SELECT blockchain
        , address
        , first_receive_from
        , first_receive_executed_by
        , amount
        , amount_usd
        , token_standard
        , token_address
        , block_time
        , block_number
        , tx_hash
        , tx_index
        , trace_address
        FROM {{ addresses_events_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
    )

SELECT MIN_BY(blockchain, block_time) AS blockchain
, address
, MIN_BY(first_receive_from, block_time) AS first_token_received_from
, array_distinct(array_agg(blockchain)) AS chains_token_received_on
, MIN_BY(first_receive_executed_by, block_time) AS first_token_received_executed_by
, MIN_BY(amount, block_time) AS amount
, MIN_BY(amount_usd, block_time) AS amount_usd
, MIN_BY(token_standard, block_time) AS token_standard
, MIN_BY(token_address, block_time) AS token_address
, MIN(block_time) AS block_time
, MIN(block_number) AS block_number
, MIN_BY(tx_hash, block_time) AS tx_hash
, MIN_BY(tx_index, block_time) AS tx_index
, MIN_BY(trace_address, block_time) AS trace_address
FROM joined_data
GROUP BY address