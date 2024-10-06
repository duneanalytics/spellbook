{{
    config(
	    schema = 'yield_yak',
        alias = 'base_trades',
        materialized = 'view',
        post_hook='{{ expose_spells(
                      blockchains = \'["arbitrum", "avalanche_c", "mantle"]\',
                      spell_type = "project",
                      spell_name = "yield_yak",
                      contributors = \'["angus_1", "Henrystats", "hosuke"]\') }}'
        )
}}

{%- set yield_yak_models = [
    ref('yield_yak_avalanche_c_base_trades'),
    ref('yield_yak_arbitrum_base_trades'),
    ref('yield_yak_mantle_base_trades')
] -%}


SELECT *
FROM (
    {%- for dex_model in yield_yak_models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , token_bought_amount_raw
        , token_sold_amount_raw
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , trace_address
        , evt_index
    FROM {{ dex_model }}
    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)
