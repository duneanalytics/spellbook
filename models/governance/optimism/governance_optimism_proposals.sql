-- Proposals Available On Agora And Snapshot Platform

{{ config(
    alias = 'proposals'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,schema = 'governance_optimism'
    ,incremental_strategy = 'merge'
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
    ,max_by(b.time, evt_block_time) as deadline
    ,max(evt_block_time) as latest_updated_at
  FROM {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalDeadlineUpdated') }} as d
  JOIN {{ source( 'optimism' , 'blocks') }} as b 
    on d.deadline = b.number
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}
  GROUP BY 1

  UNION ALL

  SELECT
    cast(proposalId as varchar) as proposal_id
    ,max_by(b.time, evt_block_time) as deadline
    ,max(evt_block_time) as latest_updated_at
  FROM {{ source('optimism_governor_optimism','OptimismGovernorV6_evt_ProposalDeadlineUpdated') }} as d
  JOIN {{ source( 'optimism' , 'blocks') }} as b 
    on d.deadline = b.number
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
  {% endif %}
  GROUP BY 1
)

WITH all_proposals AS (
    SELECT *
    FROM (
        {% for model in models %}
        SELECT
            proposal_id,
            proposal_link,
            proposal_type,
            proposal_description,
            start_block,
            start_timestamp,
            end_block,
            end_timestamp,
            platform,
            highest_weightage_vote,
            highest_weightage_voter,
            highest_weightage_voter_percentage,
            total_for_votingWeightage,
            total_abstain_votingWeightage,
            total_against_votingWeightage,
            unique_for_votes,
            unique_abstain_votes,
            unique_against_votes,
            unique_votes_count,
            total_votes_casted,
            proposal_status
        FROM
            {{ model }}
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('start_timestamp') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT *
FROM all_proposals
