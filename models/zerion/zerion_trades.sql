{{ config(
        alias='trades',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "gnosis", "avalanche_c", "arbitrum", "fantom", "optimism", "polygon"]\',
                        "project",
                        "zerion",
                        \'["hildobby"]\') }}'
        )
}}

{% set zerion_models = [
--ref('zerion_ethereum_trades')
ref('zerion_bnb_trades')
, ref('zerion_avalanche_c_trades')
, ref('zerion_gnosis_trades')
, ref('zerion_arbitrum_trades')
, ref('zerion_fantom_trades')
, ref('zerion_optimism_trades')
, ref('zerion_polygon_trades')
] %}


SELECT *
FROM (
    {% for zerion_model in zerion_models %}
    SELECT
        blockchain
        , block_time
        , block_number
        , trader
        , token_sold_address
        , token_sold_amount
        , token_bought_address
        , token_bought_amount
        , tx_from
        , tx_to
        , tx_hash
        , contract_address
        , evt_index
        , marketplace_fee
        , protocol_fee
    FROM {{ zerion_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
; 
