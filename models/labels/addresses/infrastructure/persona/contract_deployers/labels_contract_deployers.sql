{{
    config(tags=['dunesql'],
        alias = alias('contract_deployers'),
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis","goerli","optimism","polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

{% set contract_deployers_models = [
    ref('labels_contract_deployers_arbitrum')
    , ref('labels_contract_deployers_avalanche_c')
    , ref('labels_contract_deployers_bnb')
    , ref('labels_contract_deployers_ethereum')
    , ref('labels_contract_deployers_fantom')
    , ref('labels_contract_deployers_gnosis')
    , ref('labels_contract_deployers_goerli')
    , ref('labels_contract_deployers_optimism')
    , ref('labels_contract_deployers_polygon')
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