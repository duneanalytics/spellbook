{{
  config(
    schema='solana_utils',
    alias='token_accounts_alt_approach_lead_func',
    materialized='table',
    partition_by=['token_account_prefix']
  )
}}

{% set start_date = '2025-04-01' %}

-- Final model that applies the lead function to the incremental events model
WITH events_filtered AS (
  -- Pre-filter to only include records we need
  SELECT
    token_account,
    account_owner,
    account_mint,
    event_time AS valid_from,
    token_account_prefix
  FROM {{ ref('alt_solana_utils_token_accounts_events') }}
  WHERE account_owner IS NOT NULL 
    OR event_type = 'init' -- Include initializations
),

timeline AS (
  SELECT
    token_account,
    account_owner,
    account_mint,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY token_account ORDER BY valid_from) AS valid_to,
    token_account_prefix
  FROM events_filtered
)

SELECT
  token_account,
  account_owner,
  account_mint,
  valid_from,
  COALESCE(valid_to, TIMESTAMP '9999-12-31 23:59:59') AS valid_to,
  token_account_prefix
FROM timeline
WHERE account_owner IS NOT NULL AND account_mint IS NOT NULL
ORDER BY token_account, valid_from
