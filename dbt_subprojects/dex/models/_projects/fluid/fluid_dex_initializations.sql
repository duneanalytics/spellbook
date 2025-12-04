{{ config(
        schema = 'fluid',
        alias = 'dex_initializations'
        )
}}

{% set fluid_models = [
ref('fluid_arbitrum_dex_initializations')
, ref('fluid_base_dex_initializations')
, ref('fluid_ethereum_dex_initializations')
, ref('fluid_polygon_dex_initializations')
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
        , isSmartCol 
        , isSmartDebt
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)