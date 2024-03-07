{{ config(alias = 'agora_proposals'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,schema = 'governance_optimism'
    ,incremental_strategy = 'merge'
    ,unique_key = ['proposal_id']
    ,post_hook='{{ expose_spells(\'["optimism"]\',
                                      "sector",
                                      "governance",
                                    \'["chain_l"]\') }}'
    )
}}

SELECT
  CONCAT(
    SUBSTRING(
      CAST(TRY_CAST(p.proposal_id AS VARBINARY) AS VARCHAR),
      1,
      35
    ),
    '...'
  ) AS proposal_id,
  '<a href="https://vote.optimism.io/proposals/' || CAST(p.proposal_id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
  'Single-Choice Proposal' AS proposal_type, -- Set the proposal type to 'Single-Choice Proposal'
  CONCAT(
    SUBSTRING(CAST(p.proposal_description AS VARCHAR), 1, 35),
    '...'
  ) AS proposal_description,
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
  CASE
    WHEN pc.proposalId IS NOT NULL THEN 'cancelled'
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
FROM
  (
    -- Select Single-Choice proposals from Agora platform based on specific criteria
    SELECT
      proposalId AS proposal_id,
      description AS proposal_description,
      TRY_CAST(startBlock AS BIGINT) As start_block,
      TRY_CAST(endBlock AS BIGINT) As end_block,
      'agora' AS platform
    FROM
      {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCreated') }}
    WHERE
      votingModule IS NULL
      AND LOWER(description) NOT LIKE '%test vote%'
      AND NOT CAST("proposalID" AS VARCHAR) IN (
        '90839767999322802375479087567202389126141447078032129455920633707568400402209'
      )
  ) AS p
  LEFT JOIN {{ ref('governance_optimism_proposal_votes') }} AS v ON TRY_CAST(p.proposal_id AS VARBINARY) = v.proposal_id
  LEFT JOIN {{ source('optimism','blocks') }} AS s ON p.start_block = s.number
  LEFT JOIN {{ source('optimism','blocks') }} AS e ON p.end_block = e.number
  LEFT JOIN {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCanceled') }} AS pc ON p.proposal_id = pc.proposalId
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.end_block,
  p.platform,
  s.time,
  e.time,
  pc.proposalId
UNION ALL
SELECT
  CONCAT(
    SUBSTRING(
      CAST(TRY_CAST(p.proposal_id AS VARBINARY) AS VARCHAR),
      1,
      35
    ),
    '...'
  ) AS proposal_id,
  '<a href="https://vote.optimism.io/proposals/' || CAST(p.proposal_id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
  'Multi-Choice Proposal' AS proposal_type, -- Set the proposal type to 'Multi-Choice Proposal'
  CONCAT(
    SUBSTRING(CAST(p.proposal_description AS VARCHAR), 1, 35),
    '...'
  ) AS proposal_description,
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
  '' AS proposal_status
FROM
  (
    -- Select Multi-Choice proposals from agora platform based on specific criteria
    SELECT
      proposalId AS proposal_id,
      description AS proposal_description,
      TRY_CAST(startBlock AS BIGINT) As start_block,
      TRY_CAST(endBlock AS BIGINT) As end_block,
      'agora' AS platform
    FROM
      {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCreated') }}
    WHERE
      votingModule IS NOT NULL
  ) AS p
  LEFT JOIN {{ ref('governance_optimism_proposal_votes') }} AS v ON TRY_CAST(p.proposal_id AS VARBINARY) = v.proposal_id
  LEFT JOIN {{ source('optimism','blocks') }} AS s ON p.start_block = s.number
  LEFT JOIN {{ source('optimism','blocks') }} AS e ON p.end_block = e.number
  LEFT JOIN {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCanceled') }} AS pc ON p.proposal_id = pc.proposalId
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.end_block,
  p.platform,
  s.time,
  e.time,
  pc.proposalId
UNION ALL
SELECT
  CONCAT(
    SUBSTRING(
      CAST(TRY_CAST(p.proposal_id AS VARBINARY) AS VARCHAR),
      1,
      35
    ),
    '...'
  ) AS proposal_id,
  '<a href="https://vote.optimism.io/proposals/' || CAST(p.proposal_id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
  'Test Proposal' AS proposal_type, -- Set the proposal type to 'Test Proposal'
  CONCAT(
    SUBSTRING(CAST(p.proposal_description AS VARCHAR), 1, 35),
    '...'
  ) AS proposal_description,
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
  CASE
    WHEN pc.proposalId IS NOT NULL THEN 'cancelled'
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
FROM
  (
    -- Select test proposals fromagora platform based on specific criteria
    SELECT
      proposalId AS proposal_id,
      description AS proposal_description,
      TRY_CAST(startBlock AS BIGINT) As start_block,
      TRY_CAST(endBlock AS BIGINT) As end_block,
      'agora' AS platform
    FROM
      {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCreated') }}
    WHERE
      votingModule IS NULL
      AND TRY_CAST("proposalId" AS VARBINARY) IN (
        0xc8d57c95e6ac37c5d9edcbfddc2f1d8804850430c9e41c15fed373fde8bb8721,
        0xc6d51769619ecbf5e631e60d7460918f4a62823e3285a48ef6bac9e6f7be12b7,
        0x3f3bc08d05d9bde20f495e76334daeeda34ce94ed656704186693a3f2dbaa790,
        0xe50f250eed689783da7eab4b13a2c7e0dddb32dee9f3185872903a17a70e120c
      )
      --  AND LOWER(description) LIKE '%test vote%'
  ) AS p
  LEFT JOIN {{ ref('governance_optimism_proposal_votes') }} AS v ON TRY_CAST(p.proposal_id AS VARBINARY) = v.proposal_id
  LEFT JOIN {{ source('optimism','blocks') }} AS s ON p.start_block = s.number
  LEFT JOIN {{ source('optimism','blocks') }} AS e ON p.end_block = e.number
  LEFT JOIN {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCanceled') }} AS pc ON p.proposal_id = pc.proposalId
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.end_block,
  p.platform,
  s.time,
  e.time,
  pc.proposalId