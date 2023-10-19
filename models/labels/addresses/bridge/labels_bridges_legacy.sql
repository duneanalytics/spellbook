{{ config(
	tags=['legacy'],
	
    alias = alias('bridge', legacy_model=True),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "fantom"]\',
                                "sector",
                                "labels",
                                \'["ilemi"]\') }}')
}}

{% set bridges_models = [
 ref('labels_bridges_ethereum_legacy')
 , ref('labels_bridges_fantom_legacy')
] %}

SELECT *
FROM (
    {% for bridges_model in bridges_models %}
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
    FROM {{ bridges_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)