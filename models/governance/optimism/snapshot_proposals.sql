
{{ config(tags=['dunesql']
    ,alias = 'snapshot_proposals'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,schema = 'governance_optimism_snapshot_proposals'
    ,incremental_strategy = 'merge'
    ,unique_key = ['proposal_id']
    ,post_hook='{{ expose_spells(\'["optimism"]\',
                                      "sector",
                                      "governance",
                                    \'["chain_l"]\') }}'
    )
}}

SELECT
  p.proposal_id,
  'ForAgainst Proposal' AS proposal_type, -- Set the proposal type to 'ForAgainst Proposal'
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform,
  ARRAY_AGG(v.voter ORDER BY v.voter) AS voter_address,
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
  
  UNION ALL
  
  SELECT
  p.proposal_id,
  'Multiple Options Proposal' AS proposal_type, -- Set the proposal type to 'Multiple Options Proposal'
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform,
  ARRAY_AGG(v.voter ORDER BY v.voter) AS voter_address,
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
  
  SELECT
  p.proposal_id,
  'Test Proposal' AS proposal_type, -- Set the proposal type to 'Test Proposal'
  p.proposal_description,
  p.start_block,
  p.start_date,
  p.end_block,
  p.end_date,
  p.platform,
  ARRAY_AGG(v.voter ORDER BY v.voter) AS voter_address,
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
