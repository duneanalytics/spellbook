{{
    config(
	tags=['legacy'],
	
        alias = alias('contract_deployers', legacy_model=True),
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis","goerli","optimism","polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

{% set contract_deployers_models = [
    ref('labels_contract_deployers_arbitrum_legacy')
    , ref('labels_contract_deployers_avalanche_c_legacy')
    , ref('labels_contract_deployers_bnb_legacy')
    , ref('labels_contract_deployers_ethereum_legacy')
    , ref('labels_contract_deployers_fantom_legacy')
    , ref('labels_contract_deployers_gnosis_legacy')
    , ref('labels_contract_deployers_goerli_legacy')
    , ref('labels_contract_deployers_optimism_legacy')
    , ref('labels_contract_deployers_polygon_legacy')
] %}

SELECT *
FROM (
    {% for contract_deployers_model in contract_deployers_models %}
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
    FROM {{ contract_deployers_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
) AS contract_deployers