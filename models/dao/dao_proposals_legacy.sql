{{ config(
	tags=['legacy'],
	
        alias = alias('proposals', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dao",
                                \'["soispoke"]\') }}'
        )
}}

{% set dao_proposals_models = [
ref('uniswap_v3_ethereum_proposals_legacy')
, ref('compound_v2_ethereum_proposals_legacy')
, ref('gitcoin_ethereum_proposals_legacy')
, ref('ens_ethereum_proposals_legacy')
, ref('aave_ethereum_proposals_legacy')
, ref('dydx_ethereum_proposals_legacy')
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
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
