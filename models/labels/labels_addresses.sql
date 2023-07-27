{{ config(
	tags=['legacy'],
	
    alias = alias('addresses', legacy_model=True),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["bitcoin", "ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "fantom"]\',
                                "sector",
                                "labels",
                                \'["soispoke", "hildobby", "ilemi", "hosuke"]\') }}')
}}

-- single category labels (no subsets), needs label_type and model_name added still.
{% set single_category_labels_models = [
    ref('labels_aztec_v2_contracts_ethereum_legacy')
    , ref('labels_balancer_v1_pools_legacy')
    , ref('labels_balancer_v2_pools_legacy')
    , ref('labels_balancer_v2_gauges_legacy')
    , ref('labels_cex_legacy')
    , ref('labels_contracts_legacy')
    , ref('labels_hackers_ethereum_legacy')
    , ref('labels_ofac_sanctionned_ethereum_legacy')
    , ref('labels_project_wallets_legacy')
    , ref('labels_safe_ethereum_legacy')
    , ref('labels_tornado_cash_legacy')
    , ref('labels_likely_bot_labels_legacy')
    , ref('labels_quest_participants_legacy')
    , ref('labels_cex_users_legacy')
    , ref('labels_op_retropgf_legacy')
] %}

-- new/standardized labels
{% set standardized_labels_models = [
    ref('labels_bridges_legacy')
    , ref('labels_dex_legacy')
    , ref('labels_social_legacy')
    , ref('labels_nft_legacy')
    , ref('labels_airdrop_legacy')
    , ref('labels_dao_legacy')
    , ref('labels_infrastructure_legacy')
] %}


SELECT *
FROM (
    {% for single_category_labels_model in single_category_labels_models %}
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
    FROM {{ single_category_labels_model }}

    UNION ALL

    {% endfor %}

    {% for standardized_labels_model in standardized_labels_models %}
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
    FROM {{ standardized_labels_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;