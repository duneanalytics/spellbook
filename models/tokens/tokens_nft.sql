{{ config( alias = 'nft',
        post_hook='{{ expose_spells(\'["avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom","arbitrum","polygon","base","celo","zora","zksync"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}')}}


{% set sources = [
     ('arbitrum',   ref('tokens_arbitrum_nft'))
    ,('avalanche_c',ref('tokens_avalanche_c_nft'))
    ,('bnb',        ref('tokens_bnb_nft'))
    ,('ethereum',   ref('tokens_ethereum_nft'))
    ,('fantom',     ref('tokens_fantom_nft'))
    ,('gnosis',     ref('tokens_gnosis_nft'))
    ,('optimism',   ref('tokens_optimism_nft'))
    ,('polygon',    ref('tokens_polygon_nft'))
    ,('base',       ref('tokens_base_nft'))
    ,('celo',       ref('tokens_celo_nft'))
    ,('zora',       ref('tokens_zora_nft'))
    ,('zksync',     ref('tokens_zksync_nft'))
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
