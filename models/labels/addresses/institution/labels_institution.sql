{{config(alias = alias('institution'),
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "fantom", "optimism", "bitcoin", "polygon", "avalanche_c", "arbitrum"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi", "hildobby"]\') }}'
)}}

{% set institution_models = [
 ref('labels_cex')
 , ref('labels_funds')
] %}

SELECT *
FROM (
    {% for institution_model in institution_models %}
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
    FROM {{ institution_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
