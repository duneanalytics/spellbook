{{ config(
        alias ='approvals',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon", "fantom", "goerli"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}'
)
}}

{% set nft_models = [
 ref('nft_ethereum_approvals')
,ref('nft_bnb_approvals')
,ref('nft_avalanche_c_approvals')
,ref('nft_gnosis_approvals')
,ref('nft_optimism_approvals')
,ref('nft_arbitrum_approvals')
,ref('nft_polygon_approvals')
,ref('nft_fantom_approvals')
,ref('nft_goerli_approvals')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
        blockchain
        , block_time
        , block_date
        , block_number
        , address
        , token_standard
        , approval_for_all
        , contract_address
        , token_id
        , approved 
        , approved_for_all
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
        , unique_approval_id
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
