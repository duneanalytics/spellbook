
{{ config(
    alias = 'snapshot_proposals'
    ,tags = ['prod_exclude']
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
  p.proposal_id,
  '<a href="https://snapshot.org/#/opcollective.eth/proposal/' || CAST(p.proposal_id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
  'Single-Choice Proposal' AS proposal_type, -- Set the proposal type to 'Single-Choice Proposal'
  CONCAT(
    SUBSTRING(CAST(p.proposal_description AS VARCHAR), 1, 35),
    '...'
  ) AS proposal_description,
  p.start_block,
  p.start_timestamp,
  p.end_block,
  p.end_timestamp,
  p.proposal_created_at,
  p.platform,
  MAX(v.votingWeightage) AS highest_weightage_vote,
  MAX_BY(v.voter, v.votingWeightage) AS highest_weightage_voter,
  (
    MAX(v.votingWeightage) * 100 / SUM(v.votingWeightage)
  ) AS highest_weightage_voter_percentage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '1' THEN v.votingWeightage
      ELSE 0
    END
  ) AS total_for_votingWeightage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '3' THEN v.votingWeightage
      ELSE 0
    END
  ) AS total_abstain_votingWeightage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '2' THEN v.votingWeightage
      ELSE 0
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
      WHEN TRY_CAST(v.choice AS varchar) = '3' THEN 1
      ELSE 0
    END
  ) AS unique_abstain_votes,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '2' THEN 1
      ELSE 0
    END
  ) AS unique_against_votes,
  COUNT(v.choice) AS unique_votes_count,
  SUM(v.votingWeightage) AS total_votes_casted,
  COUNT(DISTINCT v.voter) AS unique_voters,
  CASE
    WHEN (
      SUM(
        CASE
          WHEN TRY_CAST(v.choice AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
          ELSE 0.0
        END
      ) / SUM(v.votingWeightage)
    ) * 100 >= 50 THEN 'success'
    WHEN p.end_timestamp > CURRENT_TIMESTAMP THEN 'active'
    ELSE 'defeated'
  END AS proposal_status
FROM
  (
    -- Select Single-Choice proposals from snapshot platform based on specific criteria
    SELECT
      cast(id as varchar) AS proposal_id,
      CONCAT(
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(title AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(' - ' AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(body AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        )
      ) AS proposal_description,
      "start" AS start_block,
      FROM_UNIXTIME("start") AS start_timestamp,
      "end" AS end_block,
      FROM_UNIXTIME("end") AS end_timestamp,
      'snapshot' AS platform,
      FROM_UNIXTIME("created") AS proposal_created_at
    FROM
      {{ source('snapshot','proposals') }}
    WHERE
      "space" = 'opcollective.eth'
      AND "type" != 'approval'
      AND "id" NOT IN (
        0x7b9a8eee9f90c7af6587afc5aef0db050c1e5ee9277d3aa18d8624976fb466bd,
        0xe4a520e923a4669fceb53c88caa13699c2fd94608df08b9a804506ac808a02f9
      )
      {% if is_incremental() %}
      AND {{ incremental_predicate('FROM_UNIXTIME("start")') }}
      {% endif %}
  ) AS p
  LEFT JOIN {{ ref('governance_optimism_proposal_votes') }} AS v ON p.proposal_id = v.proposal_id
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.start_timestamp,
  p.end_block,
  p.end_timestamp,
  p.platform,
  p.proposal_created_at
UNION ALL
SELECT
  p.proposal_id,
  '<a href="https://snapshot.org/#/opcollective.eth/proposal/' || CAST(p.proposal_id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
  'Multi-Choice Proposal' AS proposal_type, -- Set the proposal type to 'Multi-Choice Proposal'
  CONCAT(
    SUBSTRING(CAST(p.proposal_description AS VARCHAR), 1, 35),
    '...'
  ) AS proposal_description,
  p.start_block,
  p.start_timestamp,
  p.end_block,
  p.end_timestamp,
  p.proposal_created_at,
  p.platform,
  MAX(v.votingWeightage) AS highest_weightage_vote,
  MAX_BY(v.voter, v.votingWeightage) AS highest_weightage_voter,
  (
    MAX(v.votingWeightage) * 100 / SUM(v.votingWeightage)
  ) AS highest_weightage_voter_percentage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '1' THEN v.votingWeightage
      ELSE 0
    END
  ) AS total_for_votingWeightage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '3' THEN v.votingWeightage
      ELSE 0
    END
  ) AS total_abstain_votingWeightage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '2' THEN v.votingWeightage
      ELSE 0
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
      WHEN TRY_CAST(v.choice AS varchar) = '3' THEN 1
      ELSE 0
    END
  ) AS unique_abstain_votes,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '2' THEN 1
      ELSE 0
    END
  ) AS unique_against_votes,
  COUNT(v.choice) AS unique_votes_count,
  SUM(v.votingWeightage) AS total_votes_casted,
  COUNT(DISTINCT v.voter) AS unique_voters,
  '' AS proposal_status
FROM
  (
    -- Select Multi-Choice proposals from snapshot platform based on specific criteria
    SELECT
      cast(id as varchar) AS proposal_id,
      CONCAT(
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(title AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(' - ' AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(body AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        )
      ) AS proposal_description,
      "start" AS start_block,
      FROM_UNIXTIME("start") AS start_timestamp,
      "end" AS end_block,
      FROM_UNIXTIME("end") AS end_timestamp,
      'snapshot' AS platform,
      FROM_UNIXTIME("created") AS proposal_created_at
    FROM
      {{ source('snapshot','proposals') }}
    WHERE
      "space" = 'opcollective.eth'
      AND "type" = 'approval'
      {% if is_incremental() %}
      AND {{ incremental_predicate('FROM_UNIXTIME("start")') }}
      {% endif %}
  ) AS p
  LEFT JOIN {{ ref('governance_optimism_proposal_votes') }} AS v ON p.proposal_id = v.proposal_id
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.start_timestamp,
  p.end_block,
  p.end_timestamp,
  p.platform,
  p.proposal_created_at
UNION ALL
SELECT
  p.proposal_id,
  '<a href="https://snapshot.org/#/opcollective.eth/proposal/' || CAST(p.proposal_id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
  'Test Proposal' AS proposal_type, -- Set the proposal type to 'Test Proposal'
  CONCAT(
    SUBSTRING(CAST(p.proposal_description AS VARCHAR), 1, 35),
    '...'
  ) AS proposal_description,
  p.start_block,
  p.start_timestamp,
  p.end_block,
  p.end_timestamp,
  p.proposal_created_at,
  p.platform,
  MAX(v.votingWeightage) AS highest_weightage_vote,
  MAX_BY(v.voter, v.votingWeightage) AS highest_weightage_voter,
  (
    MAX(v.votingWeightage) * 100 / SUM(v.votingWeightage)
  ) AS highest_weightage_voter_percentage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '1' THEN v.votingWeightage
      ELSE 0
    END
  ) AS total_for_votingWeightage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '3' THEN v.votingWeightage
      ELSE 0
    END
  ) AS total_abstain_votingWeightage,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '2' THEN v.votingWeightage
      ELSE 0
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
      WHEN TRY_CAST(v.choice AS varchar) = '3' THEN 1
      ELSE 0
    END
  ) AS unique_abstain_votes,
  SUM(
    CASE
      WHEN TRY_CAST(v.choice AS varchar) = '2' THEN 1
      ELSE 0
    END
  ) AS unique_against_votes,
  COUNT(v.choice) AS unique_votes_count,
  SUM(v.votingWeightage) AS total_votes_casted,
  COUNT(DISTINCT v.voter) AS unique_voters,
  CASE
    WHEN (
      SUM(
        CASE
          WHEN TRY_CAST(v.choice AS varchar) = '1' THEN TRY_CAST(v.votingWeightage AS DOUBLE)
          ELSE 0.0
        END
      ) / SUM(v.votingWeightage)
    ) * 100 >= 50 THEN 'success'
    WHEN p.end_timestamp > CURRENT_TIMESTAMP THEN 'active'
    ELSE 'defeated'
  END AS proposal_status
FROM
  (
    -- Select test proposals from snapshot platform based on specific criteria
    SELECT
      cast(id as varchar) AS proposal_id,
      CONCAT(
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(title AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(' - ' AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  TRY_CAST(
                    COALESCE(
                      TRY_CAST(
                        COALESCE(
                          TRY_CAST(
                            TRY_CAST(
                              COALESCE(
                                TRY_CAST(
                                  COALESCE(
                                    TRY_CAST(
                                      TRY_CAST(
                                        COALESCE(
                                          TRY_CAST(
                                            COALESCE(
                                              TRY_CAST(
                                                TRY_CAST(
                                                  COALESCE(
                                                    TRY_CAST(
                                                      COALESCE(
                                                        TRY_CAST(
                                                          TRY_CAST(
                                                            COALESCE(
                                                              TRY_CAST(
                                                                COALESCE(TRY_CAST(body AS VARCHAR), '') AS VARCHAR
                                                              ),
                                                              ''
                                                            ) AS VARCHAR
                                                          ) AS VARCHAR
                                                        ),
                                                        ''
                                                      ) AS VARCHAR
                                                    ),
                                                    ''
                                                  ) AS VARCHAR
                                                ) AS VARCHAR
                                              ),
                                              ''
                                            ) AS VARCHAR
                                          ),
                                          ''
                                        ) AS VARCHAR
                                      ) AS VARCHAR
                                    ),
                                    ''
                                  ) AS VARCHAR
                                ),
                                ''
                              ) AS VARCHAR
                            ) AS VARCHAR
                          ),
                          ''
                        ) AS VARCHAR
                      ),
                      ''
                    ) AS VARCHAR
                  ) AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        )
      ) AS proposal_description,
      "start" AS start_block,
      FROM_UNIXTIME("start") AS start_timestamp,
      "end" AS end_block,
      FROM_UNIXTIME("end") AS end_timestamp,
      'snapshot' AS platform,
      FROM_UNIXTIME("created") AS proposal_created_at
    FROM
      {{ source('snapshot','proposals') }}
    WHERE
      "space" = 'opcollective.eth'
      AND "id" IN (
        0x7b9a8eee9f90c7af6587afc5aef0db050c1e5ee9277d3aa18d8624976fb466bd,
        0xe4a520e923a4669fceb53c88caa13699c2fd94608df08b9a804506ac808a02f9
      )
      {% if is_incremental() %}
      AND {{ incremental_predicate('FROM_UNIXTIME("start")') }}
      {% endif %}
  ) AS p
  LEFT JOIN {{ ref('governance_optimism_proposal_votes') }} AS v ON p.proposal_id = v.proposal_id
GROUP BY
  p.proposal_id,
  p.proposal_description,
  p.start_block,
  p.start_timestamp,
  p.end_block,
  p.end_timestamp,
  p.platform,
  p.proposal_created_at
