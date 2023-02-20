{{ config(
        alias ='votes',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dao",
                                \'["soispoke"]\') }}'
        )
}}

{% set dao_votes_models = [
'uniswap_v3_ethereum_votes',
'compound_v2_ethereum_votes',
'gitcoin_ethereum_votes',
'ens_ethereum_votes',
'aave_ethereum_votes'
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
    FROM {{ ref(dao_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)