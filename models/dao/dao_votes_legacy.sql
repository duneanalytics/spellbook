{{ config(
	tags=['legacy'],
	
        alias = alias('votes', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dao",
                                \'["soispoke"]\') }}'
        )
}}

{% set dao_votes_models = [
ref('uniswap_v3_ethereum_votes_legacy')
, ref('compound_v2_ethereum_votes_legacy')
, ref('gitcoin_ethereum_votes_legacy')
, ref('ens_ethereum_votes_legacy')
, ref('aave_ethereum_votes_legacy')
, ref('dydx_ethereum_votes_legacy')
] %}


SELECT *
FROM (
    {% for dao_model in dao_votes_models %}
    SELECT
        blockchain,
        project,
        version,
        block_time,
        tx_hash,
        dao_name,
        dao_address,
        proposal_id,
        votes,
        votes_share,
        token_symbol,
        token_address,
        votes_value_usd,
        voter_address,
        support,
        reason
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)