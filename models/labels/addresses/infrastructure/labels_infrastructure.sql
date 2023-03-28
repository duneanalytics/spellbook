{{ config(
    alias = 'infrastructure',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["ilemi", "hildobby"]\') }}')
}}

{% set infrastructure_models = [
 ref('labels_eth_stakers')
 , ref('labels_miners')
 , ref('labels_system_addresses')
 , ref('labels_validators')
 , ref('labels_flashbots_ethereum')
 , ref('labels_mev_ethereum')
 , ref('labels_stablecoins')
 , ref('labels_burn_addresses')
] %}

SELECT *
FROM (
    {% for infrastructure_model in infrastructure_models %}
    SELECT
        *
    FROM {{ infrastructure_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
