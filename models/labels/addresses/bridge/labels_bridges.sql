{{ config(
    alias = 'bridge',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "fantom", "base"]\',
                                "sector",
                                "labels",
                                \'["ilemi", "rantum"]\') }}')
}}

{% set bridges_models = [
 ref('labels_bridges_ethereum')
 , ref('labels_bridges_fantom')
 , ref('labels_bridges_base')
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