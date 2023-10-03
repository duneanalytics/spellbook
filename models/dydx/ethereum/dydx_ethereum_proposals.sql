{{ config(
    tags=['dunesql'],
    schema = 'dydx_ethereum',
    alias = alias('proposals'),
    materialized = 'table',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "dydx",
                                \'["ivigamberdiev"]\') }}'
    )
}}

{% set blockchain = 'ethereum' %}
{% set project = 'dydx' %}
{% set dao_name = 'DAO: dYdX' %}
{% set dao_address = '0x7e9b1672616ff6d6629ef2879419aae79a9018d2' %}

with cte_latest_block as (
SELECT MAX(b.number) AS latest_block
FROM {{ source('ethereum','blocks') }} b
),

cte_support as (SELECT
        voter as voter,
        CASE WHEN support = false THEN sum(votingPower/1e18) ELSE 0 END AS votes_against,
        CASE WHEN support = true THEN sum(votingPower/1e18) ELSE 0 END AS votes_for,
        0 AS votes_abstain,
        id
FROM {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_VoteEmitted') }}
GROUP BY support, id, voter),

cte_sum_votes as (
SELECT COUNT(DISTINCT voter) as number_of_voters,
       SUM(votes_for) as votes_for,
       SUM(votes_against) as votes_against,
       0 as votes_abstain,
       SUM(votes_for) + SUM(votes_against) + 0 as votes_total,
       id
from cte_support
GROUP BY id)

SELECT DISTINCT
    '{{blockchain}}' as blockchain,
    '{{project}}' as project,
    cast(NULL as varchar) as version,
    pcr.evt_block_time as created_at,
    date_trunc('DAY', pcr.evt_block_time) AS block_date,
    cast(date_trunc('month', pcr.evt_block_time) as date) AS block_month,
    pcr.evt_tx_hash as tx_hash, -- Proposal Created tx hash
    '{{dao_name}}' as dao_name,
    {{dao_address}} as dao_address,
    creator as proposer,
    pcr.id as proposal_id,
    csv.votes_for,
    csv.votes_against,
    csv.votes_abstain,
    csv.votes_total,
    csv.number_of_voters,
    csv.votes_total / 1e9 * 100 AS participation, -- Total votes / Total supply (1B for dYdX)
    pcr.startBlock as start_block,
    pcr.endBlock as end_block,
    CASE
         WHEN pex.id is not null and now() > pex.evt_block_time THEN 'Executed'
         WHEN pca.id is not null and now() > pca.evt_block_time THEN 'Canceled'
         WHEN (SELECT cast(latest_block as uint256) FROM cte_latest_block) <= pcr.startBlock THEN 'Pending'
         WHEN (SELECT cast(latest_block as uint256) FROM cte_latest_block) <= pcr.endBlock THEN 'Active'
         WHEN pqu.id is not null and now() > pqu.evt_block_time and now() < from_unixtime(cast(pqu.executionTime as double)) THEN 'Queued'
         ELSE 'Defeated' END AS status,
    cast(NULL as varchar) as description
FROM  {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_ProposalCreated') }} pcr
LEFT JOIN cte_sum_votes csv ON csv.id = pcr.id
LEFT JOIN {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_ProposalCanceled') }} pca ON pca.id = pcr.id
LEFT JOIN {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_ProposalExecuted') }} pex ON pex.id = pcr.id
LEFT JOIN {{ source('dydx_protocol_ethereum', 'DydxGovernor_evt_ProposalQueued') }} pqu ON pqu.id = pcr.id