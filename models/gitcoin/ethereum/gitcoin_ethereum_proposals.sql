{{ config(
    tags=['dunesql'],
    schema = 'gitcoin_ethereum',
    alias = alias('proposals'),
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

with cte_support as (SELECT 
        voter as voter,
        CASE WHEN support = FALSE THEN sum(votes/1e18) ELSE 0 END AS votes_against,
        CASE WHEN support = TRUE THEN sum(votes/1e18) ELSE 0 END AS votes_for,
        0 AS votes_abstain,
        proposalId
FROM {{ source('gitcoin_ethereum', 'GovernorAlpha_evt_VoteCast') }}
GROUP BY support, proposalId, voter),

cte_sum_votes as (
SELECT COUNT(DISTINCT voter) as number_of_voters,
       SUM(votes_for) as votes_for, 
       SUM(votes_against) as votes_against, 
       SUM(votes_abstain) as votes_abstain, 
       SUM(votes_for) + SUM(votes_against) + SUM(votes_abstain) as votes_total,
       proposalId
from cte_support
GROUP BY proposalId)

SELECT DISTINCT
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    cast(NULL as varchar) as version,
    pcr.evt_block_time as created_at,
    date_trunc('DAY', pcr.evt_block_time) AS block_date,
    pcr.evt_tx_hash as tx_hash, -- Proposal Created tx hash
    '{{dao_name}}' as dao_name,
    {{dao_address}} as dao_address,
    proposer,
    pcr.id as proposal_id,
    csv.votes_for,
    csv.votes_against,
    csv.votes_abstain,
    csv.votes_total,
    csv.number_of_voters,
    csv.votes_total / 1e9 * 100 AS participation, -- Total votes / Total supply (1B for Uniswap)
    pcr.startBlock as start_block,
    pcr.endBlock as end_block,
    CASE 
         WHEN pex.id is not null and now() > pex.evt_block_time THEN 'Executed' 
         WHEN pca.id is not null and now() > pca.evt_block_time THEN 'Canceled'
         WHEN pcr.startBlock <  cast(pcr.evt_block_number as uint256) and cast(pcr.evt_block_number as uint256) < pcr.endBlock THEN 'Active'
         WHEN now() > pqu.evt_block_time AND startBlock > cast(pcr.evt_block_number as uint256) THEN 'Queued'
         ELSE 'Defeated' END AS status,
    description as description
FROM  {{ source('gitcoin_ethereum', 'GovernorAlpha_evt_ProposalCreated') }} pcr
LEFT JOIN cte_sum_votes csv ON csv.proposalId = pcr.id
LEFT JOIN {{ source('gitcoin_ethereum', 'GovernorAlpha_evt_ProposalCanceled') }} pca ON pca.id = pcr.id
LEFT JOIN {{ source('gitcoin_ethereum', 'GovernorAlpha_evt_ProposalExecuted') }} pex ON pex.id = pcr.id
LEFT JOIN {{ source('gitcoin_ethereum', 'GovernorAlpha_evt_ProposalQueued') }} pqu ON pex.id = pcr.id