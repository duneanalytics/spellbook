{{config(
	tags=['legacy'],
	alias = alias('balancer_v2_gauges', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski"]\') }}')}}

{% set gauges_models = [
    ref('labels_balancer_v2_gauges_ethereum_legacy')
    , ref('labels_balancer_v2_gauges_polygon_legacy')
    , ref('labels_balancer_v2_gauges_arbitrum_legacy')
    , ref('labels_balancer_v2_gauges_optimism_legacy')
] %}

SELECT *
FROM (
    {% for gauges_model in gauges_models %}
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
    FROM {{ gauges_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;