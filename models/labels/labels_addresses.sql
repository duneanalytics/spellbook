{{ config(
    
    alias = 'addresses',
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["bitcoin", "ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "fantom"]\',
                                "sector",
                                "labels",
                                \'["soispoke", "hildobby", "ilemi", "hosuke", "kaiblade"]\') }}')
}}

-- single category labels (no subsets), needs label_type and model_name added still.
{% set single_category_labels_models = [
    ref('labels_aztec_v2_contracts_ethereum')
    , ref('labels_balancer_v1_pools')
    , ref('labels_balancer_v2_pools')
    , ref('labels_balancer_v2_gauges')
    , ref('labels_cex')
    , ref('labels_contracts')
    , ref('labels_hackers_ethereum')
    , ref('labels_ofac_sanctionned_ethereum')
    , ref('labels_project_wallets')
    , ref('labels_safe_ethereum')
    , ref('labels_tornado_cash')
    , ref('labels_likely_bot_labels')
    , ref('labels_quest_participants')
    , ref('labels_cex_users')
    , ref('labels_op_retropgf')
    , ref('labels_op_attestationstation_attesters')
    , ref('labels_op_attestationstation_creators')
    , ref('labels_op_governance_delegators')
    , ref('labels_op_governance_derived_archetype')
    , ref('labels_op_governance_retropgf_proposal_submitters')
    , ref('labels_op_governance_voters')
    , ref('labels_op_governance_retropgf_voters')
    , ref('labels_op_perpetual_traders')
    , ref('labels_op_traders_derived_archetype')
    , ref('labels_op_transfers_only')
] %}

-- new/standardized labels
--remove until upstream is resolved: , ref('labels_airdrop')
{% set standardized_labels_models = [
    ref('labels_bridges')
    , ref('labels_dex')
    , ref('labels_social')
    , ref('labels_nft')
    , ref('labels_dao')
    , ref('labels_infrastructure')
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