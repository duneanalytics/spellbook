{{ config(
        alias='trades',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "gnosis", "avalanche_c", "arbitrum", "fantom", "optimism", "polygon"]\',
                        "project",
                        "zerion",
                        \'["hildobby"]\') }}'
        )
}}

{% set zerion_models = [
ref('zerion_arbitrum_trades')
, ref('zerion_avalanche_c_trades')
, ref('zerion_bnb_trades')
, ref('zerion_gnosis_trades')
, ref('zerion_optimism_trades')
, ref('zerion_polygon_trades')
] %}


SELECT *
FROM (
    {% for zerion_model in zerion_models %}
    SELECT
        blockchain
        , block_time
        , block_date
        , block_number
        , trader
        , token_sold_address
        , token_sold_symbol
        , token_sold_amount_raw
        , token_sold_amount_original
        , token_bought_address
        , token_bought_symbol
        , token_bought_amount_raw
        , token_bought_amount_original
        , amount_usd
        , tx_from
        , tx_to
        , tx_hash
        , contract_address
        , evt_index
        , marketplace_fee_amount_raw
        , marketplace_fee_amount_original
        , protocol_fee_amount_raw
        , protocol_fee_amount_original
    FROM {{ zerion_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
; 
