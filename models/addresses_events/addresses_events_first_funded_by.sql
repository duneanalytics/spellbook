{{ config
(
    alias = 'first_funded_by'
    
    , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis", "optimism", "polygon", "celo", "zksync", "zora"]\',
                                    "sector",
                                    "addresses_events",
                                    \'["hildobby", "Henrystats"]\') }}'
)
}}

{% set addresses_events_models = [
ref('addresses_events_arbitrum_first_funded_by')
, ref('addresses_events_avalanche_c_first_funded_by')
, ref('addresses_events_bnb_first_funded_by')
, ref('addresses_events_ethereum_first_funded_by')
, ref('addresses_events_fantom_first_funded_by')
, ref('addresses_events_gnosis_first_funded_by')
, ref('addresses_events_optimism_first_funded_by')
, ref('addresses_events_polygon_first_funded_by')
, ref('addresses_events_celo_first_funded_by')
, ref('addresses_events_zksync_first_funded_by')
, ref('addresses_events_zora_first_funded_by')
] %}

SELECT *
FROM (
    {% for addresses_events_model in addresses_events_models %}
    SELECT blockchain
    , address
    , first_funded_by
    , block_time
    , block_number
    , tx_hash
    FROM {{ addresses_events_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
