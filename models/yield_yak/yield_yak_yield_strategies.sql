{{
    config(
	    schema = 'yield_yak',
        alias = 'yield_strategies',
        materialized = 'view',
        post_hook='{{ expose_spells(
                      blockchains = \'["arbitrum", "avalanche_c", "mantle"]\',
                      spell_type = "project",
                      spell_name = "yield_yak",
                      contributors = \'["angus_1"]\') }}'
        )
}}

{%- set yield_yak_models = [
    ref('yield_yak_avalanche_c_yield_strategies'),
    ref('yield_yak_arbitrum_yield_strategies'),
    ref('yield_yak_mantle_yield_strategies')
] -%}


SELECT *
FROM (
    {%- for strategy_model in yield_yak_models %}
    SELECT
        blockchain
        , contract_address
        , contract_name
        , created_block_time
        , created_block_number
    FROM {{ strategy_model }}
    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)
