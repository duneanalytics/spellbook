{{config(alias='balancer_v2_gauges',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["jacektrocinski"]\') }}')}}

{% set gauge_models = [
'labels_balancer_v2_gauges_ethereum'
] %}

SELECT *
FROM (
    {% for gauge_model in gauge_models %}
    SELECT
        blockchain,
        address,
        name,
        category,
        contributor,
        source,
        created_at,
        updated_at
    FROM {{ ref(gauge_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
);