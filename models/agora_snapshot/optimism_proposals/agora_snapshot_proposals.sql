-- Test Proposals Available On Agora And Snapshot Platform

{{ config(tags=['dunesql']
    ,alias = 'agora_snapshot_proposals'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,schema = 'agora_snapshot_optimism_proposals'
    ,incremental_strategy = 'merge'
    ,unique_key = ['proposal_id']
    ,post_hook='{{ expose_spells(\'["optimism"]\',
                                      "sector",
                                      "agora_snapshot",
                                    \'["chain_l"]\') }}'
    )
}}

SELECT
 p.proposal_id,
 'Test Proposal' AS proposal_type, -- Set the proposal type to 'Test Proposal'
 p.proposal_description,
 p.start_block,
 s.time AS start_date,
 p.end_block,
 e.time AS end_date,
 p.platform,
 ARRAY_AGG(v.voter ORDER BY v.voter) AS voters,
 ARRAY_AGG(TRY_CAST(v.votingWeightage AS DOUBLE) ORDER BY v.voter) AS corresponding_voting_weightage,
 ARRAY_AGG(TRY_CAST(v.support AS varchar) ORDER BY v.voter) AS corresponding_choices,
 ARRAY_AGG(v.status ORDER BY v.voter) AS corresponding_choices_name,
 MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) AS highest_weighted_vote,
 MAX_BY(v.voter, v.votingWeightage) AS highest_weighted_voter,
 CASE
  WHEN SUM(TRY_CAST(v.votingWeightage AS DOUBLE)) = 0
  THEN NULL
  ELSE (
   MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) * 100 / SUM(TRY_CAST(v.votingWeightage AS DOUBLE))
  )
 END AS highest_weighted_voter_percentage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_for_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '2' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_abstain_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '0' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_against_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '1' THEN 1 ELSE 0 END) AS unique_for_votes,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '2' THEN 1 ELSE 0 END) AS unique_abstain_votes,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '0' THEN 1 ELSE 0 END) AS unique_against_votes,
 COUNT(v.support) AS unique_votes_count,
 SUM(v.votingWeightage) AS total_votes_casted,
 CASE
  WHEN pc.proposalId IS NOT NULL
  THEN 'cancelled'
  WHEN (SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) / SUM(v.votingWeightage)) * 100 >= 50
  THEN 'success'
  WHEN e.time > CURRENT_TIMESTAMP 
  THEN 'active'
  ELSE 'defeated'
 END AS proposal_status
FROM (
-- Select test proposals from Agora platform based on specific criteria
 SELECT
  TRY_CAST(proposalId AS VARBINARY) AS proposal_id,
  description AS proposal_description,
  TRY_CAST(startBlock AS BIGINT) As start_block,
  TRY_CAST(endBlock AS BIGINT) As end_block,
  'agora' AS platform
 FROM {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCreated') }}
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
LEFT JOIN (
-- Join vote data with test proposals
 SELECT
  TRY_CAST(proposalId AS VARBINARY) AS proposal_id,
  voter,
  weight / POW(10,18) AS votingWeightage,
  support,
  CASE
   WHEN support = 0
   THEN 'against'
   WHEN support = 1
   THEN 'for'
   WHEN support = 2
   THEN 'abstain'
  END AS status
 FROM {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_VoteCast') }}
) AS v
 ON p.proposal_id = v.proposal_id
LEFT JOIN {{ source('optimism','blocks') }} AS s
 ON p.start_block = s.number
LEFT JOIN {{ source('optimism','blocks') }} AS e
 ON p.end_block = e.number
LEFT JOIN {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCanceled') }} AS pc
 ON p.proposal_id = TRY_CAST(pc.proposalId AS VARBINARY)
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

-- Select test proposals from Snapshot platform
SELECT
  p.proposal_id,
  'Test Proposal' AS proposal_type, -- Set the proposal type to 'Test Proposal'
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform,
  ARRAY_AGG(v.voter ORDER BY v.voter) AS voters,
  ARRAY_AGG(v.votingWeightage ORDER BY v.voter) AS corresponding_voting_weightage,
  ARRAY_AGG(v.choice ORDER BY v.voter) AS corresponding_choices,
  ARRAY_AGG(v.status ORDER BY v.voter) AS corresponding_choices_name,
  MAX(v.votingWeightage) AS highest_weighted_vote,
  MAX_BY(v.voter, v.votingWeightage) AS highest_weighted_voter,
  (
    MAX(v.votingWeightage) * 100 / SUM(v.votingWeightage)
  ) AS highest_weighted_voter_percentage,
  SUM(CASE WHEN v.choice = '1' THEN v.votingWeightage ELSE 0 END) AS total_for_votingWeightage,
  SUM(CASE WHEN v.choice = '3' THEN v.votingWeightage ELSE 0 END) AS total_abstain_votingWeightage,
  SUM(CASE WHEN v.choice = '2' THEN v.votingWeightage ELSE 0 END) AS total_against_votingWeightage,
  SUM(CASE WHEN v.choice = '1' THEN 1 ELSE 0 END) AS unique_for_votes,
  SUM(CASE WHEN v.choice = '3' THEN 1 ELSE 0 END) AS unique_abstain_votes,
  SUM(CASE WHEN v.choice = '2' THEN 1 ELSE 0 END) AS unique_against_votes,
  COUNT(v.choice) AS unique_votes_count,
  SUM(v.votingWeightage) AS total_votes_casted,
  CASE
  WHEN (SUM(CASE WHEN TRY_CAST(v.choice AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) / SUM(v.votingWeightage)) * 100 >= 50
  THEN 'success'
  WHEN p.end_date > CURRENT_TIMESTAMP  
  THEN 'active'
  ELSE 'defeated'
 END AS proposal_status
FROM (
  -- Select test proposals from Snapshot platform based on specific criteria
  SELECT
    id AS proposal_id,
    CONCAT(
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(title AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR),
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(' - ' AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR),
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(body AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR)
    ) AS proposal_description,
    start AS start_block,
    FROM_UNIXTIME(start) AS start_date,
    "end" AS end_block,
    FROM_UNIXTIME("end") AS end_date,
    'snapshot' AS platform
  FROM {{ source('snapshot','proposals') }}
  WHERE
    "space" = 'opcollective.eth'
    AND "id" IN (0x7b9a8eee9f90c7af6587afc5aef0db050c1e5ee9277d3aa18d8624976fb466bd,0xe4a520e923a4669fceb53c88caa13699c2fd94608df08b9a804506ac808a02f9)
) AS p
LEFT JOIN (
-- Join vote data with test proposals
  SELECT
    proposal AS proposal_id,
    voter,
    vp AS votingWeightage,
    choice,
    CASE
      WHEN choice = '1'
      THEN 'for'
      WHEN choice = '2'
      THEN 'against'
      WHEN choice = '3'
      THEN 'abstain'
    END AS status
  FROM {{ source('snapshot','votes') }}
  WHERE
    "space" = 'opcollective.eth'
) AS v
  ON p.proposal_id = v.proposal_id
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform

UNION ALL

-- Multiple Options Proposals Available On Agora And Snapshot Platform
SELECT
 p.proposal_id,
 'Multiple Options Proposal' AS proposal_type, -- Set the proposal type to 'Multiple Options Proposal'
 p.proposal_description,
 p.start_block,
 s.time AS start_date,
 p.end_block,
 e.time AS end_date,
 p.platform,
 ARRAY_AGG(v.voter ORDER BY v.voter) AS voters,
 ARRAY_AGG(TRY_CAST(v.votingWeightage AS DOUBLE) ORDER BY v.voter) AS corresponding_voting_weightage,
 ARRAY_AGG(TRY_CAST(v.support AS varchar) ORDER BY v.voter) AS corresponding_choices,
 ARRAY_AGG(v.status ORDER BY v.voter) AS corresponding_choices_name,
 MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) AS highest_weighted_vote,
 MAX_BY(v.voter, v.votingWeightage) AS highest_weighted_voter,
 CASE
  WHEN SUM(TRY_CAST(v.votingWeightage AS DOUBLE)) = 0
  THEN NULL
  ELSE (
   MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) * 100 / SUM(TRY_CAST(v.votingWeightage AS DOUBLE))
  )
 END AS highest_weighted_voter_percentage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_for_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '2' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_abstain_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '0' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_against_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '1' THEN 1 ELSE 0 END) AS unique_for_votes,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '2' THEN 1 ELSE 0 END) AS unique_abstain_votes,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '0' THEN 1 ELSE 0 END) AS unique_against_votes,
 COUNT(v.support) AS unique_votes_count,
 SUM(v.votingWeightage) AS total_votes_casted,
 '' AS proposal_status
FROM (
-- Select multiple options proposals from Agora platform based on specific criteria
 SELECT
  TRY_CAST(proposalId AS VARBINARY) AS proposal_id,
  description AS proposal_description,
  TRY_CAST(startBlock AS BIGINT) As start_block,
  TRY_CAST(endBlock AS BIGINT) As end_block,
  'agora' AS platform
 FROM {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCreated') }}
 WHERE votingModule IS NOT NULL
) AS p
LEFT JOIN (
-- Join vote data with multiple options proposals
 SELECT
  TRY_CAST(proposalId AS VARBINARY) AS proposal_id,
  voter,
  weight / POW(10,18) AS votingWeightage,
  support,
  CASE
   WHEN support = 0
   THEN 'against'
   WHEN support = 1
   THEN 'for'
   WHEN support = 2
   THEN 'abstain'
  END AS status
 FROM {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_VoteCast') }}
) AS v
 ON p.proposal_id = v.proposal_id
LEFT JOIN {{ source('optimism','blocks') }} AS s
 ON p.start_block = s.number
LEFT JOIN {{ source('optimism','blocks') }} AS e
 ON p.end_block = e.number
LEFT JOIN {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCanceled') }} AS pc
 ON p.proposal_id = TRY_CAST(pc.proposalId AS VARBINARY)
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

-- Select multiple options proposals from Snapshot platform
SELECT
  p.proposal_id,
  'Multiple Options Proposal' AS proposal_type, -- Set the proposal type to 'Multiple Options Proposal'
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform,
  ARRAY_AGG(v.voter ORDER BY v.voter) AS voters,
  ARRAY_AGG(v.votingWeightage ORDER BY v.voter) AS corresponding_voting_weightage,
  ARRAY_AGG(v.choice ORDER BY v.voter) AS corresponding_choices,
  ARRAY_AGG(v.status ORDER BY v.voter) AS corresponding_choices_name,
  MAX(v.votingWeightage) AS highest_weighted_vote,
  MAX_BY(v.voter, v.votingWeightage) AS highest_weighted_voter,
  (
    MAX(v.votingWeightage) * 100 / SUM(v.votingWeightage)
  ) AS highest_weighted_voter_percentage,
  SUM(CASE WHEN v.choice = '1' THEN v.votingWeightage ELSE 0 END) AS total_for_votingWeightage,
  SUM(CASE WHEN v.choice = '3' THEN v.votingWeightage ELSE 0 END) AS total_abstain_votingWeightage,
  SUM(CASE WHEN v.choice = '2' THEN v.votingWeightage ELSE 0 END) AS total_against_votingWeightage,
  SUM(CASE WHEN v.choice = '1' THEN 1 ELSE 0 END) AS unique_for_votes,
  SUM(CASE WHEN v.choice = '3' THEN 1 ELSE 0 END) AS unique_abstain_votes,
  SUM(CASE WHEN v.choice = '2' THEN 1 ELSE 0 END) AS unique_against_votes,
  COUNT(v.choice) AS unique_votes_count,
  SUM(v.votingWeightage) AS total_votes_casted,
  '' AS proposal_status
FROM (
-- Select multiple options proposals from Snapshot platform based on specific criteria
  SELECT
    id AS proposal_id,
    CONCAT(
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(title AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR),
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(' - ' AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR),
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(body AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR)
    ) AS proposal_description,
    start AS start_block,
    FROM_UNIXTIME(start) AS start_date,
    "end" AS end_block,
    FROM_UNIXTIME("end") AS end_date,
    'snapshot' AS platform
  FROM {{ source('snapshot','proposals') }}
  WHERE
    "space" = 'opcollective.eth'
    AND "type" = 'approval'
) AS p
LEFT JOIN (
-- Join vote data with multiple options proposals
  SELECT
    proposal AS proposal_id,
    voter,
    vp AS votingWeightage,
    choice,
    CASE
      WHEN choice = '1'
      THEN 'for'
      WHEN choice = '2'
      THEN 'against'
      WHEN choice = '3'
      THEN 'abstain'
    END AS status
  FROM {{ source('snapshot','votes') }}
  WHERE
    "space" = 'opcollective.eth'
) AS v
  ON p.proposal_id = v.proposal_id
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform

UNION ALL

-- ForAgainst Proposals Available On Agora And Snapshot Platform
SELECT
 p.proposal_id,
 'ForAgainst Proposal' AS proposal_type, -- Set the proposal type to 'ForAgainst Proposal'
 p.proposal_description,
 p.start_block,
 s.time AS start_date,
 p.end_block,
 e.time AS end_date,
 p.platform,
 ARRAY_AGG(v.voter ORDER BY v.voter) AS voters,
 ARRAY_AGG(TRY_CAST(v.votingWeightage AS DOUBLE) ORDER BY v.voter) AS corresponding_voting_weightage,
 ARRAY_AGG(TRY_CAST(v.support AS varchar) ORDER BY v.voter) AS corresponding_choices,
 ARRAY_AGG(v.status ORDER BY v.voter) AS corresponding_choices_name,
 MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) AS highest_weighted_vote,
 MAX_BY(v.voter, v.votingWeightage) AS highest_weighted_voter,
 CASE
  WHEN SUM(TRY_CAST(v.votingWeightage AS DOUBLE)) = 0
  THEN NULL
  ELSE (
   MAX(TRY_CAST(v.votingWeightage AS DOUBLE)) * 100 / SUM(TRY_CAST(v.votingWeightage AS DOUBLE))
  )
 END AS highest_weighted_voter_percentage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_for_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '2' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_abstain_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '0' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) AS total_against_votingWeightage,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '1' THEN 1 ELSE 0 END) AS unique_for_votes,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '2' THEN 1 ELSE 0 END) AS unique_abstain_votes,
 SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '0' THEN 1 ELSE 0 END) AS unique_against_votes,
 COUNT(v.support) AS unique_votes_count,
 SUM(v.votingWeightage) AS total_votes_casted,
 CASE
  WHEN pc.proposalId IS NOT NULL
  THEN 'cancelled'
  WHEN (SUM(CASE WHEN TRY_CAST(v.support AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) / SUM(v.votingWeightage)) * 100 >= 50
  THEN 'success'
  WHEN e.time > CURRENT_TIMESTAMP 
  THEN 'active'
  ELSE 'defeated'
 END AS proposal_status
FROM (
-- Select ForAgainst proposals from Agora platform based on specific criteria
 SELECT
  TRY_CAST(proposalId AS VARBINARY) AS proposal_id,
  description AS proposal_description,
  TRY_CAST(startBlock AS BIGINT) As start_block,
  TRY_CAST(endBlock AS BIGINT) As end_block,
  'agora' AS platform
 FROM {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCreated') }}
 WHERE votingModule IS NULL
 AND LOWER(description) NOT LIKE '%test vote%'
 AND NOT CAST("proposalID" AS VARCHAR) IN ('90839767999322802375479087567202389126141447078032129455920633707568400402209')
) AS p
LEFT JOIN (
-- Join vote data with ForAgainst proposals
 SELECT
  TRY_CAST(proposalId AS VARBINARY) AS proposal_id,
  voter,
  weight / POW(10,18) AS votingWeightage,
  support,
  CASE
   WHEN support = 0
   THEN 'against'
   WHEN support = 1
   THEN 'for'
   WHEN support = 2
   THEN 'abstain'
  END AS status
 FROM {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_VoteCast') }}
) AS v
 ON p.proposal_id = v.proposal_id
LEFT JOIN {{ source('optimism','blocks') }} AS s
 ON p.start_block = s.number
LEFT JOIN {{ source('optimism','blocks') }} AS e
 ON p.end_block = e.number
LEFT JOIN {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_ProposalCanceled') }} AS pc
 ON p.proposal_id = TRY_CAST(pc.proposalId AS VARBINARY)
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

-- Select ForAgainst proposals from Snapshot platform
SELECT
  p.proposal_id,
  'ForAgainst Proposal' AS proposal_type, -- Set the proposal type to 'ForAgainst Proposal'
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform,
  ARRAY_AGG(v.voter ORDER BY v.voter) AS voters,
  ARRAY_AGG(v.votingWeightage ORDER BY v.voter) AS corresponding_voting_weightage,
  ARRAY_AGG(v.choice ORDER BY v.voter) AS corresponding_choices,
  ARRAY_AGG(v.status ORDER BY v.voter) AS corresponding_choices_name,
  MAX(v.votingWeightage) AS highest_weighted_vote,
  MAX_BY(v.voter, v.votingWeightage) AS highest_weighted_voter,
  (
    MAX(v.votingWeightage) * 100 / SUM(v.votingWeightage)
  ) AS highest_weighted_voter_percentage,
  SUM(CASE WHEN v.choice = '1' THEN v.votingWeightage ELSE 0 END) AS total_for_votingWeightage,
  SUM(CASE WHEN v.choice = '3' THEN v.votingWeightage ELSE 0 END) AS total_abstain_votingWeightage,
  SUM(CASE WHEN v.choice = '2' THEN v.votingWeightage ELSE 0 END) AS total_against_votingWeightage,
  SUM(CASE WHEN v.choice = '1' THEN 1 ELSE 0 END) AS unique_for_votes,
  SUM(CASE WHEN v.choice = '3' THEN 1 ELSE 0 END) AS unique_abstain_votes,
  SUM(CASE WHEN v.choice = '2' THEN 1 ELSE 0 END) AS unique_against_votes,
  COUNT(v.choice) AS unique_votes_count,
  SUM(v.votingWeightage) AS total_votes_casted,
  CASE
  WHEN (SUM(CASE WHEN TRY_CAST(v.choice AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE) ELSE 0.0 END) / SUM(v.votingWeightage)) * 100 >= 50
  THEN 'success'
  WHEN p.end_date > CURRENT_TIMESTAMP  
  THEN 'active'
  ELSE 'defeated'
 END AS proposal_status
FROM (
-- Select ForAgainst proposals from Snapshot platform based on specific criteria
  SELECT
    id AS proposal_id,
    CONCAT(
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(title AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR),
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(' - ' AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR),
      CAST(COALESCE(
        CAST(COALESCE(
          TRY_CAST(TRY_CAST(COALESCE(
            TRY_CAST(COALESCE(
              TRY_CAST(TRY_CAST(COALESCE(
                TRY_CAST(COALESCE(
                  TRY_CAST(TRY_CAST(COALESCE(
                    TRY_CAST(COALESCE(
                      TRY_CAST(TRY_CAST(COALESCE(
                        TRY_CAST(COALESCE(
                          TRY_CAST(TRY_CAST(COALESCE(TRY_CAST(COALESCE(TRY_CAST(body AS VARCHAR), '') AS VARCHAR), '') AS VARCHAR) AS VARCHAR),
                          ''
                        ) AS VARCHAR),
                        ''
                      ) AS VARCHAR) AS VARCHAR),
                      ''
                    ) AS VARCHAR),
                    ''
                  ) AS VARCHAR) AS VARCHAR),
                  ''
                ) AS VARCHAR),
                ''
              ) AS VARCHAR) AS VARCHAR),
              ''
            ) AS VARCHAR),
            ''
          ) AS VARCHAR) AS VARCHAR),
          ''
        ) AS VARCHAR),
        ''
      ) AS VARCHAR)
    ) AS proposal_description,
    start AS start_block,
    FROM_UNIXTIME(start) AS start_date,
    "end" AS end_block,
    FROM_UNIXTIME("end") AS end_date,
    'snapshot' AS platform
  FROM {{ source('snapshot','proposals') }}
  WHERE
    "space" = 'opcollective.eth'
    AND "type" != 'approval'
    AND "id" NOT IN (0x7b9a8eee9f90c7af6587afc5aef0db050c1e5ee9277d3aa18d8624976fb466bd,0xe4a520e923a4669fceb53c88caa13699c2fd94608df08b9a804506ac808a02f9)
) AS p
LEFT JOIN (
-- Join vote data with ForAgainst proposals
  SELECT
    proposal AS proposal_id,
    voter,
    vp AS votingWeightage,
    choice,
    CASE
      WHEN choice = '1'
      THEN 'for'
      WHEN choice = '2'
      THEN 'against'
      WHEN choice = '3'
      THEN 'abstain'
    END AS status
  FROM {{ source('snapshot','votes') }}
  WHERE
    "space" = 'opcollective.eth'
) AS v
  ON p.proposal_id = v.proposal_id
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform