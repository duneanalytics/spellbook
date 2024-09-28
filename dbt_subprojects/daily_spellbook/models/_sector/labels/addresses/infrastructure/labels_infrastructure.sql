{{ config(
    alias = 'infrastructure',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["ilemi", "hildobby"]\') }}')
}}

--exclude due to duplicates:
 --ref('labels_eth_stakers') 

{% set infrastructure_models = [
    ref('labels_miners')
    , ref('labels_system_addresses')
    , ref('labels_validators')
    , ref('labels_flashbots_ethereum')
    , ref('labels_mev_ethereum')
    , ref('labels_contract_deployers')
    , ref('labels_stablecoins')
    , ref('labels_cex_tokens')
    , ref('labels_burn_addresses')
    , ref('labels_flashloans_ethereum')
] %}

SELECT *
FROM (
    {% for infrastructure_model in infrastructure_models %}
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
    FROM {{ infrastructure_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
