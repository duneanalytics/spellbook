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
        *
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    
    {% endif %}
    {% endfor %}
)
