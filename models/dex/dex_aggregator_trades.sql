
{{ config(
        schema ='dex_aggregator',
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum", "gnosis", "avalanche_c", "fantom"]\',
                                "sector",
                                "dex_aggregator",
                                \'["bh2smith", "Henrystats", "jeff-dude"]\') }}'
        )
}}

{% set dex_aggregator_models = [
 ref('cow_protocol_trades')
 ,ref('openocean_trades')
 ,ref('paraswap_trades')
 ,ref('lifi_trades')
 ,ref('odos_trades')
 ,ref('yield_yak_avalanche_c_trades')
] %}


SELECT *
FROM (
    {% for aggregator_model in dex_aggregator_models %}
    SELECT
          blockchain
         , project
         , version
         , block_date
         , block_time
         , token_bought_symbol
         , token_sold_symbol
         , token_pair
         , token_bought_amount
         , token_sold_amount
         , token_bought_amount_raw
         , token_sold_amount_raw
         , amount_usd
         , token_bought_address
         , token_sold_address
         , taker
         , maker
         , project_contract_address
         , tx_hash
         , tx_from
         , tx_to
         , trace_address --ensure field is explicitly cast as array<bigint> in base models
         , evt_index
    FROM {{ aggregator_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;