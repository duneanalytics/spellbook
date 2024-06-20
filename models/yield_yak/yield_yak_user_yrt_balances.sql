{{
    config(
	    schema = 'yield_yak',
        alias = 'user_yrt_balances',
        materialized = 'view',
        post_hook='{{ expose_spells(
                      blockchains = \'["arbitrum", "avalanche_c", "mantle"]\',
                      spell_type = "project",
                      spell_name = "yield_yak",
                      contributors = \'["angus_1"]\') }}'
        )
}}

{%- set yield_yak_models = [
    ref('yield_yak_avalanche_c_user_yrt_balances'),
    ref('yield_yak_arbitrum_user_yrt_balances'),
    ref('yield_yak_mantle_user_yrt_balances')
] -%}


SELECT *
FROM (
    {%- for balances_model in yield_yak_models %}
    SELECT
        blockchain
        , user_address
        , contract_address
        , from_time
        , to_time
        , yrt_balance
    FROM {{ balances_model }}
    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)
