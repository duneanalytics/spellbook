{{ config(
    alias = 'proposal_votes'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,schema = 'governance_optimism'
    ,incremental_strategy = 'merge'
    ,unique_key = ['proposal_id','date_timestamp','tx_hash','voter','choice']
    ,post_hook='{{ expose_spells(\'["optimism"]\',
                                      "sector",
                                      "governance",
                                    \'["chain_l", "chuxin"]\') }}'
    )
}}

SELECT
  CAST(proposalId as VARCHAR) AS proposal_id,
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
  END AS choice_name,
  NULL as params
FROM
  {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_VoteCast') }}
{% if is_incremental() %}
WHERE
    {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

SELECT
  CAST(proposalId as VARCHAR) AS proposal_id,
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
  ,params
FROM
  {{ source('optimism_governor_optimism','OptimismGovernorV5_evt_VoteCastWithParams') }}
{% if is_incremental() %}
WHERE
    {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

SELECT
  CAST(proposalId as VARCHAR) AS proposal_id,
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
  END AS choice_name,
  NULL as params
FROM
  {{ source('optimism_governor_optimism','OptimismGovernorV6_evt_VoteCast') }}
{% if is_incremental() %}
WHERE
    {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

SELECT
  CAST(proposalId as VARCHAR) AS proposal_id,
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
  ,params
FROM
  {{ source('optimism_governor_optimism','OptimismGovernorV6_evt_VoteCastWithParams') }}
{% if is_incremental() %}
WHERE
    {{ incremental_predicate('evt_block_time') }}
{% endif %}

UNION ALL

SELECT
  TRY_CAST(proposal as VARCHAR) AS proposal_id,
  'snapshot' AS platform,
  NULL AS tx_hash,
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
  END AS choice_name,
  NULL as params
FROM
  {{ source('snapshot','votes') }}
WHERE
  "space" = 'opcollective.eth'
{% if is_incremental() %}
AND {{ incremental_predicate('from_unixtime(created)') }}
{% endif %}
