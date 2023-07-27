{{ config(
	tags=['legacy'],
	
        alias = alias('trades', legacy_model=True),
        post_hook='{{ expose_spells(\'["bnb", "gnosis", "avalanche_c", "arbitrum", "optimism", "polygon"]\',
                        "project",
                        "zerion",
                        \'["hildobby"]\') }}'
        )
}}

{% set zerion_models = [
ref('zerion_arbitrum_trades_legacy')
, ref('zerion_avalanche_c_trades_legacy')
, ref('zerion_bnb_trades_legacy')
, ref('zerion_gnosis_trades_legacy')
, ref('zerion_optimism_trades_legacy')
, ref('zerion_polygon_trades_legacy')
, ref('zerion_fantom_trades_legacy')
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
        , zerion_fee_amount_raw
        , zerion_fee_amount_original
    FROM {{ zerion_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
; 
