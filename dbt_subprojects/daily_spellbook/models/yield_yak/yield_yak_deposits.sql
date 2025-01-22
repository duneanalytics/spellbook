{{
    config(
	    schema = 'yield_yak',
        alias = 'deposits',
        materialized = 'view',
        post_hook='{{ expose_spells(
                      blockchains = \'["arbitrum", "avalanche_c", "mantle"]\',
                      spell_type = "project",
                      spell_name = "yield_yak",
                      contributors = \'["angus_1"]\') }}'
        )
}}

{%- set yield_yak_models = [
    ref('yield_yak_avalanche_c_deposits'),
    ref('yield_yak_arbitrum_deposits'),
    ref('yield_yak_mantle_deposits')
] -%}


SELECT *
FROM (
    {%- for deposits_model in yield_yak_models %}
    SELECT
        blockchain
        , contract_address
        , tx_hash
        , evt_index
        , tx_index
        , block_time
        , block_date
        , block_number
        , user_address
        , deposit_amount
    FROM {{ deposits_model }}
    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)
