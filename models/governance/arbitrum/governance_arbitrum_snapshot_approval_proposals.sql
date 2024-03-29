{{ config(alias = 'snapshot_approval_proposals'
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
  dv.choice as voting_choice,
  TRY_CAST(TRIM(TRIM(t.value, '[]'), ' ') AS INTEGER) AS voting_choice_separated,
  ELEMENT_AT(
    SPLIT(TRIM(TRIM(dp.choices, '[]'), ' '), ', '),
    TRY_CAST(TRIM(TRIM(t.value, '[]'), ' ') AS INTEGER)
  ) AS separated_voting_choice_name,
  dv.reason AS voting_reason,
  FROM_UNIXTIME(dv.created) AS voting_timestamp,
  YEAR(FROM_UNIXTIME(dv.created)) AS voting_year,
  MONTH(FROM_UNIXTIME(dv.created)) AS voting_month,
  DAY_OF_MONTH(FROM_UNIXTIME(dv.created)) AS voting_date
FROM {{ source("shot","dataset_proposals_view", database="dune") }} AS dp
JOIN {{ source("shot","dataset_votes_view", database="dune") }} AS dv
  ON dp.id = dv.proposal
CROSS JOIN UNNEST(SPLIT(dv.choice, ',')) AS t(value)
WHERE
  dp."space" = 'arbitrumfoundation.eth' AND dp."type" = 'approval'
  
  

