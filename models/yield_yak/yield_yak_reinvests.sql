{{ config(
	    schema = 'yield_yak',
        alias = 'reinvests',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c"]\',
                                "project",
                                "yield_yak",
                                \'["angus_1"]\') }}'
        )
}}

{%- set yield_yak_models = [
ref('yield_yak_avalanche_c_reinvests')
,ref('yield_yak_arbitrum_reinvests')
] -%}


SELECT *
FROM (
    {%- for reinvests_model in yield_yak_models %}
    SELECT
        blockchain
        , contract_address
        , tx_hash
        , evt_index
        , tx_index
        , block_time
        , block_number
        , reinvest_by_address
        , new_total_deposits
        , new_total_supply
        , apy
        , ratio
        , recent_reinvest_info
    FROM {{ reinvests_model }}
    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)
