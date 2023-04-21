{{ config(
    alias = 'blacklisted',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["hildobby"]\') }}')
}}

{% set blacklist_models = [
 ref('labels_tether_blacklist')
--, ref('labels_tether_blacklist')
] %}

SELECT *
FROM (
    {% for blacklist_model in blacklist_models %}
    SELECT
        *
    FROM {{ blacklist_model }}
    {% if not loop.last %}
    UNION ALL
    
    {% endif %}
    {% endfor %}
)