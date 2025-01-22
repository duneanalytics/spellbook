{{
    config(
	    schema = 'yield_yak',
        alias = 'balances',
        materialized = 'view',
        post_hook='{{ expose_spells(
                      blockchains = \'["arbitrum", "avalanche_c", "mantle"]\',
                      spell_type = "project",
                      spell_name = "yield_yak",
                      contributors = \'["angus_1"]\') }}'
        )
}}

{%- set yield_yak_models = [
    ref('yield_yak_avalanche_c_balances'),
    ref('yield_yak_arbitrum_balances'),
    ref('yield_yak_mantle_balances')
] -%}


SELECT *
FROM (
    {%- for balances_model in yield_yak_models %}
    SELECT
        blockchain
        , contract_address
        , from_time
        , to_time
        , deposit_token_balance
    FROM {{ balances_model }}
    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)
