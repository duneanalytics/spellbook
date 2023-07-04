{{ config( alias = alias('nft', legacy_model=True),
        post_hook='{{ expose_spells(\'["avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom","arbitrum","polygon"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}')}}


{% set sources = [
     ('arbitrum',   ref('tokens_arbitrum_nft_legacy'))
    ,('avalanche_c',ref('tokens_avalanche_c_nft_legacy'))
    ,('bnb',        ref('tokens_bnb_nft_legacy'))
    ,('ethereum',   ref('tokens_ethereum_nft_legacy'))
    ,('fantom',     ref('tokens_fantom_nft_legacy'))
    ,('gnosis',     ref('tokens_gnosis_nft_legacy'))
    ,('optimism',   ref('tokens_optimism_nft_legacy'))
    ,('polygon',    ref('tokens_polygon_nft_legacy'))
] %}

SELECT *
FROM (
    {% for source in sources %}
    SELECT
    '{{ source[0] }}' as blockchain,
    contract_address,
    name,
    symbol,
    standard
    FROM {{ source[1] }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
