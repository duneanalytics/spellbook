{{ config(
    alias = 'all',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["ilemi"]\') }}')
}}

{% set dao_models = [
--identifier
 ref('labels_dao_multisig_ethereum')
--Usage
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
