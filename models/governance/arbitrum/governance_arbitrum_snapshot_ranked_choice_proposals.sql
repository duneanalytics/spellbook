{{ config(alias = 'snapshot_ranked_choice_proposals'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,schema = 'governance_arbitrum'
    ,incremental_strategy = 'merge'
    ,unique_key = ['proposal_id']
    ,post_hook='{{ expose_spells(\'["arbitrum"]\',
                                      "sector",
                                      "governance",
                                    \'["ARDev097"]\') }}'
    )
}}

SELECT
  dp.id AS proposal_id,
  dp.title AS proposal_title,
  CONCAT(CAST(SUBSTRING(dp.body, 1, 40) AS VARCHAR), CAST('..' AS VARCHAR)) AS proposal_description,
  '<a href="https://snapshot.org/#/arbitrumfoundation.eth/proposal/' || CAST(dp.id AS varchar) || '" target="_blank">To Read More</a>' AS proposal_link,
  dp."type" AS proposal_type,
  dp.choices AS proposal_choice,
  dv.voter AS voter_address,
  dv.vp AS voting_weightage,
  dv.choice AS voting_choice,
  dv.reason AS voting_reason,
  FROM_UNIXTIME(dv.created) AS voting_timestamp,
  YEAR(FROM_UNIXTIME(dv.created)) AS voting_year,
  MONTH(FROM_UNIXTIME(dv.created)) AS voting_month,
  DAY_OF_MONTH(FROM_UNIXTIME(dv.created)) AS voting_date
FROM {{ source('dune.shot','dataset_proposals_view') }} AS dp
JOIN {{ source('dune.shot','dataset_votes_view') }} AS dv
  ON dp.id = dv.proposal
WHERE
  dp."space" = 'arbitrumfoundation.eth' AND dp."type" = 'ranked-choice'
