{{ config(
	tags=['legacy'],
	
    alias = alias('infrastructure', legacy_model=True),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "labels",
                                \'["ilemi", "hildobby"]\') }}')
}}

{% set infrastructure_models = [
 ref('labels_eth_stakers_legacy')
 , ref('labels_miners_legacy')
 , ref('labels_system_addresses_legacy')
 , ref('labels_validators_legacy')
 , ref('labels_flashbots_ethereum_legacy')
 , ref('labels_mev_ethereum_legacy')
 , ref('labels_contract_deployers_legacy')
 , ref('labels_stablecoins_legacy')
 , ref('labels_cex_tokens_legacy')
 , ref('labels_burn_addresses_legacy')
 , ref('labels_flashloans_ethereum_legacy')
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
