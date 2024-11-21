{{
    config(
	    schema = 'yield_yak',
        alias = 'yak_swaps',
        materialized = 'view',
        post_hook='{{ expose_spells(
                      blockchains = \'["arbitrum", "avalanche_c", "mantle"]\',
                      spell_type = "project",
                      spell_name = "yield_yak",
                      contributors = \'["angus_1"]\') }}'
        )
}}

{%- set yield_yak_models = [
    ref('yield_yak_avalanche_c_yak_swaps'),
    ref('yield_yak_arbitrum_yak_swaps'),
    ref('yield_yak_mantle_yak_swaps')
] -%}


SELECT *
FROM (
    {%- for swaps_model in yield_yak_models %}
    SELECT
        blockchain
        , yak_router_address
        , block_number
        , block_date
        , block_time
        , tx_index
        , tx_hash
        , tx_to_address
        , tx_fee
        , evt_index
        , number_of_swaps_in_tx
        , tx_fee_per_swap
        , trader_address
        , swap_token_in_address
        , swap_token_out_address
        , swap_amount_in
        , swap_amount_out
        , number_of_hops
        , yak_adapter_swaps
    FROM {{ swaps_model }}
    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {%- endfor %}
)
