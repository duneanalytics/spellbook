{{config(
    tags = ['dunesql'],
    alias = alias('cex')
)}}


{% set cex_label_models = [
ref('labels_cex_arbitrum')
, ref('labels_cex_avalanche_c')
, ref('labels_cex_bitcoin')
, ref('labels_cex_bnb')
, ref('labels_cex_ethereum')
, ref('labels_cex_fantom')
, ref('labels_cex_optimism')
, ref('labels_cex_polygon')
] %}

SELECT *
FROM (
    {% for cex_label_model in cex_label_models %}
    SELECT
    blockchain
    , address
    , name
    , category
    , contributor
    , source
    , created_at
    , updated_at
    , model_name
    , label_type
    FROM {{ cex_label_model }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)