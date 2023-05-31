{{ config(
    schema = 'uniswap_v3_ethereum',
    alias = 'votes',
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap_v3",
                                \'["soispoke"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'uniswap' %}
{% set project_version = 'v3' %}
{% set dao_name = 'DAO: Uniswap' %}
{% set dao_address = '0x408ed6354d4973f66138c91495f2f2fcbd8724c3' %}

WITH cte_sum_votes as 
(SELECT sum(votes/1e18) as sum_votes, 
        proposalId
FROM {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_VoteCast') }}
GROUP BY proposalId)

SELECT 
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    '{{project_version}}' as version,
    vc.evt_block_time as block_time,
    date_trunc('DAY', vc.evt_block_time) AS block_date,
    vc.evt_tx_hash as tx_hash,
    '{{dao_name}}' as dao_name,
    '{{dao_address}}' as dao_address,
    vc.proposalId as proposal_id,
    vc.votes/1e18 as votes,
    (votes/1e18) * (100) / (csv.sum_votes) as votes_share,
    p.symbol as token_symbol,
    p.contract_address as token_address, 
    vc.votes/1e18 * p.price as votes_value_usd,
    vc.voter as voter_address,
    CASE WHEN vc.support = 0 THEN 'against'
         WHEN vc.support = 1 THEN 'for'
         WHEN vc.support = 2 THEN 'abstain'
         END AS support,
    vc.reason
FROM {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_VoteCast') }} vc
LEFT JOIN cte_sum_votes csv ON vc.proposalId = csv.proposalId
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', evt_block_time)
    AND p.symbol = 'UNI'
    AND p.blockchain ='ethereum'