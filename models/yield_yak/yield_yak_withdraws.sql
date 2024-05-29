{{ config(
	schema = 'yield_yak',
        alias = 'withdraws',
        post_hook='{{ expose_spells(
                      blockchains = \'["arbitrum", "avalanche_c"]\',
                      spell_type = "project",
                      spell_name = "yield_yak",
                      contributors = \'["angus_1"]\') }}'
        )
}}

{%- set yield_yak_models = [
ref('yield_yak_avalanche_c_withdraws')
,ref('yield_yak_arbitrum_withdraws')
] -%}


SELECT *
FROM (
    {%- for withdraws_model in yield_yak_models %}
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
        , withdraw_amount
    FROM {{ withdraws_model }}
    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)
