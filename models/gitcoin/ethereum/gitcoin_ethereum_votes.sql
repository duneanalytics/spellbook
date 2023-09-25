{{ config(
    tags=['dunesql'],
    schema = 'gitcoin_ethereum',
    alias = alias('votes'),
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "gitcoin",
                                \'["soispoke"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'gitcoin' %}
{% set dao_name = 'DAO: Gitcoin' %}
{% set dao_address = '0xdbd27635a534a3d3169ef0498beb56fb9c937489' %}

WITH cte_sum_votes as 
(SELECT sum(votes/1e18) as sum_votes, 
        proposalId
FROM {{ source('gitcoin_ethereum', 'GovernorAlpha_evt_VoteCast') }}
GROUP BY proposalId)

SELECT 
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    cast(NULL as varchar) as version,
    vc.evt_block_time as block_time,
    date_trunc('DAY', vc.evt_block_time) AS block_date,
    vc.evt_tx_hash as tx_hash,
    '{{dao_name}}' as dao_name,
    {{dao_address}} as dao_address,
    vc.proposalId as proposal_id,
    vc.votes/1e18 as votes,
    (votes/1e18) * (100) / (csv.sum_votes) as votes_share,
    p.symbol as token_symbol,
    p.contract_address as token_address, 
    vc.votes/1e18 * p.price as votes_value_usd,
    vc.voter as voter_address,
    CASE WHEN vc.support = false THEN 'against'
         WHEN vc.support = true THEN 'for'
         END AS support,
    cast(NULL as varchar) as reason
FROM {{ source('gitcoin_ethereum', 'GovernorAlpha_evt_VoteCast') }} vc
LEFT JOIN cte_sum_votes csv ON vc.proposalId = csv.proposalId
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', evt_block_time)
    AND p.symbol = 'GTC'
    AND p.blockchain ='ethereum'