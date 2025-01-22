{{ config(
        
        schema = 'nft',
        alias = 'aggregators',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","blast","bnb","celo","ethereum","linea","mantle","optimism","polygon","scroll","zksync"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke","hildobby", "chuxin"]\') }}')
}}


{% set aggregator_models = [
        ('arbitrum', ref('nft_arbitrum_aggregators'))
        , ('avalanche_c', ref('nft_avalanche_c_aggregators'))
        , ('base', ref('nft_base_aggregators'))
        , ('blast', ref('nft_blast_aggregators'))
        , ('bnb', ref('nft_bnb_aggregators'))
        , ('celo', ref('nft_celo_aggregators'))
        , ('ethereum', ref('nft_ethereum_aggregators'))
        , ('linea', ref('nft_linea_aggregators'))
        , ('mantle', ref('nft_mantle_aggregators'))
        , ('optimism', ref('nft_optimism_aggregators'))
        , ('polygon', ref('nft_polygon_aggregators'))
        , ('scroll', ref('nft_scroll_aggregators'))
        , ('zksync', ref('nft_zksync_aggregators'))
] %}

SELECT * FROM  (
{% for aggregator_model in aggregator_models %}
    SELECT '{{ aggregator_model[0] }}' AS blockchain
    , contract_address
    , "name"
    FROM {{ aggregator_model[1] }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )