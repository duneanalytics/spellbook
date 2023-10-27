{{ config(
    
    alias = 'dao',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["ilemi"]\') }}')
}}

{% set dao_models = [
 ref('labels_dao_multisig_ethereum')
 ,ref('labels_dao_framework')
] %}

SELECT *
FROM (
    {% for dao_model in dao_models %}
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
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
