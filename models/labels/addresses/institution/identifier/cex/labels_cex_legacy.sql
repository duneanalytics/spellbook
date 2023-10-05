{{config(
	tags=['legacy'],
	alias = alias('cex', legacy_model=True))}}


{% set cex_label_models = [
ref('labels_cex_arbitrum_legacy')
, ref('labels_cex_avalanche_c_legacy')
, ref('labels_cex_bitcoin_legacy')
, ref('labels_cex_bnb_legacy')
, ref('labels_cex_ethereum_legacy')
, ref('labels_cex_fantom_legacy')
, ref('labels_cex_optimism_legacy')
, ref('labels_cex_polygon_legacy')
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