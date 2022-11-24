{{ config(
        alias ='proposals',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dao",
                                \'["soispoke"]\') }}'
        )
}}

{% set dao_proposals_models = [
'uniswap_v3_ethereum_proposals',
'compound_v2_ethereum_proposals',
'gitcoin_ethereum_proposals',
'ens_ethereum_proposals',
'aave_ethereum_proposals'
] %}

SELECT *
FROM (
    {% for dao_model in dao_proposals_models %}
    SELECT
        blockchain,
        project,
        version,
        created_at,
        tx_hash,
        dao_name,
        dao_address,
        proposer,
        proposal_id,
        votes_for,
        votes_against,
        votes_abstain,
        votes_total,
        number_of_voters,
        participation,
        status,
        description
    FROM {{ ref(dao_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
