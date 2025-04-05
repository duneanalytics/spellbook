{{
  config(
    schema='solana_utils',
    alias='token_accounts_alt_approach_lead_func',
    materialized='table'
  )
}}

{% set start_date = '2025-04-01' %}

-- Final model that applies the lead function to the incremental events model
WITH timeline AS (
  SELECT
    token_account,
    account_owner,
    account_mint,
    event_time AS valid_from,
    LEAD(event_time) OVER (PARTITION BY token_account ORDER BY event_time) AS valid_to
  FROM {{ ref('alt_solana_utils_token_accounts_events') }}
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
