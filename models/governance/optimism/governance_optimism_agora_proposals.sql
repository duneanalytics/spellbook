{{ config(
    alias = 'agora_proposals'
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
-- v5
SELECT
    p.proposal_id,
    p.proposal_created_at,
    '<a href="https://vote.optimism.io/proposals/' || CAST(p.proposal_id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
    CASE 
      WHEN votingModule IS NULL AND LOWER(proposal_description) NOT LIKE '%test vote%' THEN 'Single-Choice Proposal' 
      WHEN votingModule IS NOT NULL THEN 'Multi-Choice Proposal'
    END AS proposal_type,
    p.proposal_description,
    p.start_block,
    s.time AS start_timestamp,
    p.end_block,
    e.time AS end_timestamp,
    p.platform,
    MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) AS highest_weightage_vote,
    MAX_BY(v.voter, v.votingWeightage) AS highest_weightage_voter,
    CASE
      WHEN SUM(TRY_CAST(v.votingWeightage AS DOUBLE)) = 0 THEN NULL
      ELSE (
        MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) * 100 / SUM(TRY_CAST(v.votingWeightage AS DOUBLE))
      )
    END AS highest_weightage_voter_percentage,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
        ELSE 0.0
      END
    ) AS total_for_votingWeightage,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '2' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
        ELSE 0.0
      END
    ) AS total_abstain_votingWeightage,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '0' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
        ELSE 0.0
      END
    ) AS total_against_votingWeightage,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '1' THEN 1
        ELSE 0
      END
    ) AS unique_for_votes,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '2' THEN 1
        ELSE 0
      END
    ) AS unique_abstain_votes,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '0' THEN 1
        ELSE 0
      END
    ) AS unique_against_votes,
    COUNT(v.choice) AS unique_votes_count,
    SUM(v.votingWeightage) AS total_votes_casted,
    COUNT(DISTINCT v.voter) AS unique_voters,
    CASE
      WHEN pc.proposalId IS NOT NULL THEN 'cancelled'
      WHEN votingModule IS NOT NULL THEN NULL
      WHEN e.time IS NULL THEN 'active'
      WHEN (
        SUM(
          CASE
            WHEN TRY_CAST(v.choice AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
            ELSE 0.0
          END
        ) / SUM(v.votingWeightage)
      ) * 100 >= 50 THEN 'success'
      ELSE 'defeated'
    END AS proposal_status
  FROM (
    SELECT
      CAST(proposalId AS VARCHAR) AS proposal_id,
      description AS proposal_description,
      TRY_CAST(startBlock AS BIGINT) As start_block,
      TRY_CAST(endBlock AS BIGINT) As end_block,
      'agora' AS platform,
      votingModule,
      evt_block_time as proposal_created_at
    FROM
      {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCreated') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
  ) as p
  LEFT JOIN {{ ref('governance_optimism_proposal_votes') }} AS v ON p.proposal_id = v.proposal_id
  LEFT JOIN {{ source('optimism','blocks') }} AS s ON p.start_block = s.number
  LEFT JOIN {{ source('optimism','blocks') }} AS e ON p.end_block = e.number
  LEFT JOIN {{ source('optimism_governor_optimism','OptimismGovernorV6_evt_ProposalCanceled') }} AS pc ON p.proposal_id = cast(pc.proposalId as varchar)
  GROUP BY
    p.proposal_id,
    p.proposal_description,
    p.start_block,
    p.end_block,
    p.platform,
    s.time,
    e.time,
    pc.proposalId,
    p.votingModule,
    p.proposal_created_at

  UNION ALL
  
  -- v6
  SELECT
    p.proposal_id,
    p.proposal_created_at,
    '<a href="https://vote.optimism.io/proposals/' || CAST(p.proposal_id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
    CASE 
      WHEN votingModule IS NULL AND LOWER(proposal_description) NOT LIKE '%test vote%' THEN 'Single-Choice Proposal' 
      WHEN votingModule IS NOT NULL THEN 'Multi-Choice Proposal'
    END AS proposal_type,
    p.proposal_description,
    p.start_block,
    s.time AS start_timestamp,
    p.end_block,
    e.time AS end_timestamp,
    p.platform,
    MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) AS highest_weightage_vote,
    MAX_BY(v.voter, v.votingWeightage) AS highest_weightage_voter,
    CASE
      WHEN SUM(TRY_CAST(v.votingWeightage AS DOUBLE)) = 0 THEN NULL
      ELSE (
        MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) * 100 / SUM(TRY_CAST(v.votingWeightage AS DOUBLE))
      )
    END AS highest_weightage_voter_percentage,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
        ELSE 0.0
      END
    ) AS total_for_votingWeightage,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '2' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
        ELSE 0.0
      END
    ) AS total_abstain_votingWeightage,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '0' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
        ELSE 0.0
      END
    ) AS total_against_votingWeightage,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '1' THEN 1
        ELSE 0
      END
    ) AS unique_for_votes,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '2' THEN 1
        ELSE 0
      END
    ) AS unique_abstain_votes,
    SUM(
      CASE
        WHEN TRY_CAST(v.choice AS varchar) = '0' THEN 1
        ELSE 0
      END
    ) AS unique_against_votes,
    COUNT(v.choice) AS unique_votes_count,
    SUM(v.votingWeightage) AS total_votes_casted,
    COUNT(DISTINCT v.voter) AS unique_voters,
    CASE
      WHEN pc.proposalId IS NOT NULL THEN 'cancelled'
      WHEN votingModule IS NOT NULL THEN NULL
      WHEN e.time IS NULL THEN 'active'
      WHEN (
        SUM(
          CASE
            WHEN TRY_CAST(v.choice AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
            ELSE 0.0
          END
        ) / SUM(v.votingWeightage)
      ) * 100 >= 50 THEN 'success'
      ELSE 'defeated'
    END AS proposal_status
  FROM (
    SELECT
      CAST(proposalId AS VARCHAR) AS proposal_id,
      description AS proposal_description,
      TRY_CAST(startBlock AS BIGINT) As start_block,
      TRY_CAST(endBlock AS BIGINT) As end_block,
      'agora' AS platform,
      votingModule,
      evt_block_time as proposal_created_at
    FROM
      {{ source('optimism_governor_optimism','OptimismGovernorV6_evt_ProposalCreated') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
  ) as p
  LEFT JOIN {{ ref('governance_optimism_proposal_votes') }} AS v ON p.proposal_id = v.proposal_id
  LEFT JOIN {{ source('optimism','blocks') }} AS s ON p.start_block = s.number
  LEFT JOIN {{ source('optimism','blocks') }} AS e ON p.end_block = e.number
  LEFT JOIN {{ source('optimism_governor_optimism','OptimismGovernorV6_evt_ProposalCanceled') }} AS pc ON p.proposal_id = cast(pc.proposalId as varchar)
  GROUP BY
    p.proposal_id,
    p.proposal_description,
    p.start_block,
    p.end_block,
    p.platform,
    s.time,
    e.time,
    pc.proposalId,
    p.votingModule,
    p.proposal_created_at
