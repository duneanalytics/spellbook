{{ config(alias = 'proposal_votes'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,schema = 'governance_optimism'
    ,incremental_strategy = 'merge'
    ,unique_key = ['proposal_id','date_timestamp','tx_hash','voter','choice']
    ,post_hook='{{ expose_spells(\'["optimism"]\',
                                      "sector",
                                      "governance",
                                    \'["chain_l"]\') }}'
    )
}}

SELECT
  TRY_CAST(proposalId AS VARBINARY) AS proposal_id,
  'agora' AS platform,
  evt_tx_hash AS tx_hash,
  evt_block_time AS date_timestamp,
  voter,
  reason,
  weight / POWER(10, 18) AS votingWeightage,
  support AS choice,
  CASE
    WHEN support = 0 THEN 'against'
    WHEN support = 1 THEN 'for'
    WHEN support = 2 THEN 'abstain'
    WHEN support = 3 THEN 'voted'
  END AS choice_name
FROM
  {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_VoteCast') }}
UNION ALL
SELECT
  proposal AS proposal_id,
  'snapshot' AS platform,
  TRY_CAST('' AS VARBINARY) AS tx_hash,
  from_unixtime(created) AS date_timestamp,
  voter,
  reason,
  vp AS votingWeightage,
  TRY_CAST(choice AS INT) AS choice,
  CASE
    WHEN choice = '1' THEN 'for'
    WHEN choice = '2' THEN 'against'
    WHEN choice = '3' THEN 'abstain'
    WHEN choice = '4' THEN 'voted'
  END AS choice_name
FROM
  {{ source('snapshot','votes') }}
WHERE
  "space" = 'opcollective.eth'