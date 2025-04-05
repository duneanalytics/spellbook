{{
  config(
    tags=['prod_exclude'],
    schema='solana_utils',
    alias='token_accounts_alt_approach_self_join',
    materialized='table'
  )
}}

{% set start_date = '2025-01-01' %}

WITH initializations AS (
  SELECT
    account_account AS token_account,
    account_owner,
    account_mint,
    call_block_time AS event_time,
    'init' AS event_type
  FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount') }}
  WHERE call_block_time >= timestamp '{{start_date}}'

  UNION ALL

  SELECT
    account_account,
    owner as account_owner,
    account_mint,
    call_block_time,
    'init'
  FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount2') }}
  WHERE call_block_time >= timestamp '{{start_date}}'

  UNION ALL

  SELECT
    account_account,
    owner as account_owner,
    account_mint,
    call_block_time,
    'init'
  FROM {{ source('spl_token_solana', 'spl_token_call_initializeaccount3') }}
  WHERE call_block_time >= timestamp '{{start_date}}'
),

latest_init_before_transfer AS (
  SELECT
    i.token_account,
    i.account_mint,
    sa.call_block_time AS transfer_time,
    MAX(i.event_time) AS latest_init_time
  FROM {{ source('spl_token_solana', 'spl_token_call_setauthority') }} sa
  LEFT JOIN initializations i 
    ON sa.account_owned = i.token_account
    AND i.event_time < sa.call_block_time
  WHERE json_extract_scalar(sa.authorityType, '$.AuthorityType') = 'AccountOwner'
    AND sa.call_block_time >= timestamp '{{start_date}}'
  GROUP BY i.token_account, i.account_mint, sa.call_block_time
),

latest_init_before_closure AS (
  SELECT
    i.token_account,
    i.account_mint,
    c.call_block_time AS closure_time,
    MAX(i.event_time) AS latest_init_time
  FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }} c
  LEFT JOIN initializations i 
    ON c.account_account = i.token_account
    AND i.event_time < c.call_block_time
  WHERE c.call_block_time >= timestamp '{{start_date}}'
  GROUP BY i.token_account, i.account_mint, c.call_block_time
),

ownership_transfers AS (
  SELECT
    sa.account_owned AS token_account,
    sa.newAuthority AS account_owner,
    li.account_mint, -- get mint from latest initialization
    sa.call_block_time AS event_time,
    'owner_change' AS event_type
  FROM {{ source('spl_token_solana', 'spl_token_call_setauthority') }} sa
  LEFT JOIN latest_init_before_transfer li 
    ON sa.account_owned = li.token_account
    AND sa.call_block_time = li.transfer_time
  WHERE json_extract_scalar(sa.authorityType, '$.AuthorityType') = 'AccountOwner'
    AND sa.call_block_time >= timestamp '{{start_date}}'
),

closures AS (
  SELECT
    c.account_account AS token_account,
    NULL AS account_owner,
    li.account_mint, -- get mint from latest initialization
    c.call_block_time AS event_time,
    'close' AS event_type
  FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }} c
  LEFT JOIN latest_init_before_closure li 
    ON c.account_account = li.token_account
    AND c.call_block_time = li.closure_time
  WHERE c.call_block_time >= timestamp '{{start_date}}'
),

all_events AS (
  SELECT * FROM initializations
  UNION ALL
  SELECT * FROM ownership_transfers
  UNION ALL
  SELECT * FROM closures
),

-- Add row numbers to enable self-join
numbered_events AS (
  SELECT
    token_account,
    account_owner,
    account_mint,
    event_time,
    ROW_NUMBER() OVER (PARTITION BY token_account ORDER BY event_time) AS event_seq
  FROM all_events
),

-- Self-join to find the next event time
timeline AS (
  SELECT
    e1.token_account,
    e1.account_owner,
    e1.account_mint,
    e1.event_time AS valid_from,
    MIN(e2.event_time) AS valid_to
  FROM numbered_events e1
  LEFT JOIN numbered_events e2 ON 
    e1.token_account = e2.token_account AND
    e1.event_seq < e2.event_seq
  GROUP BY
    e1.token_account,
    e1.account_owner,
    e1.account_mint,
    e1.event_time
)

SELECT
  token_account,
  account_owner,
  account_mint,
  valid_from,
  COALESCE(valid_to, TIMESTAMP '9999-12-31 23:59:59') AS valid_to
FROM timeline
WHERE account_owner IS NOT NULL AND account_mint IS NOT NULL
ORDER BY token_account, valid_from
