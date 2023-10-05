{{ config(
        alias = alias('votes'),
        tags = ['dunesql'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dao",
                                \'["soispoke"]\') }}'
        )
}}

{% set dao_votes_models = [
ref('uniswap_v3_ethereum_votes')
, ref('compound_v2_ethereum_votes')
, ref('gitcoin_ethereum_votes')
, ref('ens_ethereum_votes')
, ref('aave_ethereum_votes')
, ref('dydx_ethereum_votes')
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