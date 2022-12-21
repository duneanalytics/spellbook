{{ config(
        alias ='transfers',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "avalanche_c", "gnosis", "optimism", "arbitrum", "polygon"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby", "0xRob"]\') }}'
)
}}

{% set nft_models = [
 ref('nft_ethereum_transfers')
,ref('nft_bnb_transfers')
,ref('nft_avalanche_c_transfers')
,ref('nft_gnosis_transfers')
,ref('nft_optimism_transfers')
,ref('nft_arbitrum_transfers')
,ref('nft_polygon_transfers')
] %}

SELECT *
FROM (
    {% for nft_model in nft_models %}
    SELECT
          blockchain
        , block_time
        , block_date
        , block_number
        , token_standard
        , transfer_type
        , evt_index
        , contract_address
        , token_id
        , amount
        , `from`
        , to
        , tx_hash
        , unique_transfer_id
    FROM {{ nft_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
