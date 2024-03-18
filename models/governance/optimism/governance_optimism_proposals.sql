-- Proposals Available On Agora And Snapshot Platform

{{ config(
    alias = 'proposals'
    ,file_format = 'delta'
    ,schema = 'governance_optimism'
    ,unique_key = ['proposal_id']
    ,post_hook='{{ expose_spells(\'["optimism"]\',
                                      "sector",
                                      "governance",
                                    \'["chain_l", "chuxin"]\') }}'
    )
}}

{% set models = [
    ref('governance_optimism_agora_proposals'),
    ref('governance_optimism_snapshot_proposals')
] %}

WITH latest_deadline AS (
  SELECT
    cast(proposalId as varchar) as proposal_id
    ,d.deadline as deadline_block
    ,max_by(b.time, evt_block_time) as deadline
    ,max(evt_block_time) as latest_updated_at
  FROM {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalDeadlineUpdated') }} as d
  JOIN {{ source( 'optimism' , 'blocks') }} as b 
    on d.deadline = b.number
  GROUP BY 1, 2

  UNION ALL

  SELECT
    cast(proposalId as varchar) as proposal_id
    ,d.deadline as deadline_block
    ,max_by(b.time, evt_block_time) as deadline
    ,max(evt_block_time) as latest_updated_at
  FROM {{ source('optimism_governor_optimism','OptimismGovernorV6_evt_ProposalDeadlineUpdated') }} as d
  JOIN {{ source( 'optimism' , 'blocks') }} as b 
    on d.deadline = b.number
  GROUP BY 1, 2
)

,all_proposals AS (
    SELECT *
    FROM (
        {% for model in models %}
        SELECT
            m.proposal_id,
            m.proposal_link,
            m.proposal_type,
            m.proposal_description,
            m.proposal_created_at,
            m.start_block,
            m.start_timestamp,
            coalesce(d.deadline_block, m.end_block) as end_block,
            coalesce(d.deadline, m.end_timestamp) as end_timestamp,
            m.platform,
            m.highest_weightage_vote,
            m.highest_weightage_voter,
            m.highest_weightage_voter_percentage,
            m.total_for_votingWeightage,
            m.total_abstain_votingWeightage,
            m.total_against_votingWeightage,
            m.unique_for_votes,
            m.unique_abstain_votes,
            m.unique_against_votes,
            m.unique_votes_count,
            m.total_votes_casted,
            m.unique_voters,
            m.proposal_status
        FROM
            {{ model }} as m
        LEFT JOIN latest_deadline as d
            ON m.proposal_id = d.proposal_id
            AND d.latest_updated_at > m.proposal_created_at
            AND d.deadline != m.end_timestamp
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT *
FROM all_proposals
