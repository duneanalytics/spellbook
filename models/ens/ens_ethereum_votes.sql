{{ config(
    schema = 'ens_ethereum',
    alias = 'votes',
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "ens",
                                \'["soispoke"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'ens' %}
{% set dao_name = 'DAO: ENS' %}
{% set dao_address = '0x323a76393544d5ecca80cd6ef2a560c6a395b7e3' %}

WITH cte_sum_votes as 
(SELECT sum(weight/1e18) as sum_votes, 
        proposalId
FROM {{ source('ethereumnameservice_ethereum', 'ENSGovernor_evt_VoteCast') }}
GROUP BY proposalId)

SELECT 
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    cast(NULL as string) as version,
    vc.evt_block_time as block_time,
    date_trunc('DAY', vc.evt_block_time) AS block_date,
    vc.evt_tx_hash as tx_hash,
    '{{dao_name}}' as dao_name,
    '{{dao_address}}' as dao_address,
    vc.proposalId as proposal_id,
    vc.weight/1e18 as votes,
    (weight/1e18) * (100) / (csv.sum_votes) as votes_share,
    p.symbol as token_symbol,
    p.contract_address as token_address, 
    vc.weight/1e18 * p.price as votes_value_usd,
    vc.voter as voter_address,
    CASE WHEN vc.support = 0 THEN 'against'
         WHEN vc.support = 1 THEN 'for'
         WHEN vc.support = 2 THEN 'abstain'
         END AS support,
    reason
FROM {{ source('ethereumnameservice_ethereum', 'ENSGovernor_evt_VoteCast') }} vc
LEFT JOIN cte_sum_votes csv ON vc.proposalId = csv.proposalId
LEFT JOIN {{ source('prices', 'usd') }} p ON p.minute = date_trunc('minute', evt_block_time)
    AND p.symbol = 'ENS'
    AND p.blockchain ='ethereum'