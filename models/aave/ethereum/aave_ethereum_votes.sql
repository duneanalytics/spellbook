{{ config(
    schema = 'aave_ethereum',
    alias = alias('votes'),
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "aave_ethereum",
                                \'["soispoke"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'aave' %}
{% set dao_name = 'DAO: AAVE' %}
{% set dao_address = '0xec568fffba86c094cf06b22134b23074dfe2252c' %}

WITH cte_sum_votes as 
(SELECT sum(votingPower/1e18) as sum_votes, 
        id
FROM {{ source('aave_ethereum', 'AaveGovernanceV2_evt_VoteEmitted') }}
GROUP BY id)

SELECT 
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    cast(NULL as string) as version,
    vc.evt_block_time as block_time,
    date_trunc('DAY', vc.evt_block_time) AS block_date,
    vc.evt_tx_hash as tx_hash,
    '{{dao_name}}' as dao_name,
    '{{dao_address}}' as dao_address,
    vc.id as proposal_id,
    vc.votingPower/1e18 as votes,
    (votingPower/1e18) * (100) / (csv.sum_votes) as votes_share,
    p.symbol as token_symbol,
    p.contract_address as token_address, 
    vc.votingPower/1e18 * p.price as votes_value_usd,
    vc.voter as voter_address,
    CASE WHEN vc.support = 0 THEN 'against'
         WHEN vc.support = 1 THEN 'for'
         WHEN vc.support = 2 THEN 'abstain'
         END AS support,
    cast(NULL as string) as reason
FROM {{ source('aave_ethereum', 'AaveGovernanceV2_evt_VoteEmitted') }} vc
LEFT JOIN cte_sum_votes csv ON vc.id = csv.id
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', evt_block_time)
    AND p.symbol = 'AAVE'
    AND p.blockchain ='ethereum'