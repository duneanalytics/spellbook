{{ config(
        alias ='collections',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}',
        unique_key = ['unique_trade_id']
)
}}

{% set nft_models = [
 ref('nft_ethereum_collections')
,ref('nft_bnb_collections')
,ref('nft_avalanche_c_collections')
,ref('nft_gnosis_collections')
,ref('nft_optimism_collections')
,ref('nft_arbitrum_collections')
,ref('nft_polygon_collections')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT *
    FROM {{ nft_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
);
