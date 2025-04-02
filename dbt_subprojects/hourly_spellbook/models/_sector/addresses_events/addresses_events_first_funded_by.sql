{{ config
(
    alias = 'first_funded_by',
    schema = 'addresses_events',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis", "optimism", "polygon", "celo", "zora", "base", "scroll", "zksync", "sei", "mantle", "blast", "ronin", "nova"]\',
                                    "sector",
                                    "addresses_events",
                                    \'["hildobby"]\') }}'
)
}}

{% set addresses_events_models = [
(ref('addresses_events_arbitrum_first_funded_by'), 'ETH')
, (ref('addresses_events_avalanche_c_first_funded_by'), 'AVAX')
, (ref('addresses_events_bnb_first_funded_by'), 'BNB')
, (ref('addresses_events_ethereum_first_funded_by'), 'ETH')
, (ref('addresses_events_fantom_first_funded_by'), 'FTM')
, (ref('addresses_events_gnosis_first_funded_by'), 'xDAI')
, (ref('addresses_events_optimism_first_funded_by'), 'ETH')
, (ref('addresses_events_polygon_first_funded_by'), 'MATIC')
, (ref('addresses_events_celo_first_funded_by'), 'CELO')
, (ref('addresses_events_zora_first_funded_by'), 'ETH')
, (ref('addresses_events_base_first_funded_by'), 'ETH')
, (ref('addresses_events_scroll_first_funded_by'), 'ETH')
, (ref('addresses_events_zkevm_first_funded_by'), 'ETH')
, (ref('addresses_events_linea_first_funded_by'), 'ETH')
, (ref('addresses_events_sei_first_funded_by'), 'SEI')
, (ref('addresses_events_mantle_first_funded_by'), 'MNT')
, (ref('addresses_events_zksync_first_funded_by'), 'ETH')
, (ref('addresses_events_blast_first_funded_by'), 'ETH')
, (ref('addresses_events_ronin_first_funded_by'), 'RON')
, (ref('addresses_events_nova_first_funded_by'), 'ETH')
] %}

WITH joined_data AS (
    SELECT *
    FROM (
        {% for addresses_events_model in addresses_events_models %}
        SELECT blockchain
        , address
        , '{{ addresses_events_model[1] }}' AS token_symbol
        , first_funded_by
        , first_funding_executed_by
        , amount
        , amount_usd
        , block_time
        , block_number
        , tx_hash
        , tx_index
        , trace_address
        FROM {{ addresses_events_model[0] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )
    )

SELECT MIN_BY(blockchain, block_time) AS blockchain
, address
, MIN_BY(first_funded_by, block_time) AS first_funded_by
, array_distinct(array_agg(blockchain)) AS chains_funded_on
, MIN_BY(first_funding_executed_by, block_time) AS first_funding_executed_by
, MIN_BY(amount, block_time) AS amount
, MIN_BY(amount_usd, block_time) AS amount_usd
, MIN_BY(token_symbol, block_time) AS token_symbol
, MIN(block_time) AS block_time
, MIN(block_number) AS block_number
, MIN_BY(tx_hash, block_time) AS tx_hash
, MIN_BY(tx_index, block_time) AS tx_index
, MIN_BY(trace_address, block_time) AS trace_address
FROM joined_data
GROUP BY address