{{ config(
    
    schema = 'uniswap_v3_ethereum',
    alias = 'proposals',
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

with cte_support as (SELECT
        voter as voter,
        CASE WHEN support = 0 THEN sum(votes/1e18) ELSE 0 END AS votes_against,
        CASE WHEN support = 1 THEN sum(votes/1e18) ELSE 0 END AS votes_for,
        CASE WHEN support = 2 THEN sum(votes/1e18) ELSE 0 END AS votes_abstain,
        proposalId
FROM {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_VoteCast') }}
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
    '{{project_version}}' as version,
    pcr.evt_block_time as created_at,
    date_trunc('DAY', pcr.evt_block_time) AS block_date,
    CAST(date_trunc('month', pcr.evt_block_time) AS date) AS block_month,
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
    pcr.startblock as start_block,
    pcr.endblock as end_block,
    CASE
        WHEN pex.id is not null and now() > pex.evt_block_time THEN 'Executed'
        WHEN pca.id is not null and now() > pca.evt_block_time THEN 'Canceled'
        WHEN cast(pcr.startblock as bigint) < pcr.evt_block_number AND pcr.evt_block_number < cast(pcr.endblock as bigint) THEN 'Active'
        WHEN now() > pqu.evt_block_time AND cast(pcr.startblock as bigint) > pcr.evt_block_number THEN 'Queued'
        ELSE 'Defeated'
    END AS status,
    description
FROM  {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_ProposalCreated') }} pcr
LEFT JOIN cte_sum_votes csv ON csv.proposalId = pcr.id
LEFT JOIN {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_ProposalCanceled') }} pca ON pca.id = pcr.id
LEFT JOIN {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_ProposalExecuted') }} pex ON pex.id = pcr.id
LEFT JOIN {{ source('uniswap_v3_ethereum', 'GovernorBravoDelegate_evt_ProposalQueued') }} pqu ON pex.id = pcr.id
