{{ config(
        schema = 'fluid',
        alias = 'dex_fee_updates'
        )
}}

{% set fluid_models = [
ref('fluid_arbitrum_dex_fee_updates')
, ref('fluid_base_dex_fee_updates')
, ref('fluid_ethereum_dex_fee_updates')
, ref('fluid_polygon_dex_fee_updates')
] %}


SELECT *
FROM (
    {% for dex_pool_model in fluid_models %}
    SELECT
        blockchain
        , version 
        , project 
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , dex 
        , dex_id 
        , fee 
        , revenue_cut 
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)