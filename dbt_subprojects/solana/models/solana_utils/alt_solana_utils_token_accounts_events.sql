{{
  config(
    schema='solana_utils',
    alias='token_accounts_events',
    materialized='incremental',
    file_format='delta',
    partition_by=['token_account_prefix'],
    incremental_strategy='merge',
    incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.event_time')],
    unique_key=['token_account', 'event_time', 'event_type']
  )
}}

{% set start_date = '2025-04-01' %}

WITH initializations AS (
  -- Use the pre-materialized initializations table
  SELECT
    token_account,
    account_owner,
    account_mint,
    event_time,
    'init' AS event_type,
    token_account_prefix
  FROM {{ ref('alt_solana_utils_token_account_initializations') }}
),

latest_init_before_transfer AS (
  SELECT
    i.token_account,
    i.account_mint,
    sa.call_block_time AS transfer_time,
    MAX(i.event_time) AS latest_init_time
  FROM {{ source('spl_token_solana', 'spl_token_call_setauthority') }} sa
  -- Join to ALL initializations, not just recent ones
  LEFT JOIN {{ ref('alt_solana_utils_token_account_initializations') }} i 
    ON sa.account_owned = i.token_account
    AND i.event_time < sa.call_block_time
  WHERE json_extract_scalar(sa.authorityType, '$.AuthorityType') = 'AccountOwner'
    AND sa.call_block_time >= timestamp '{{start_date}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('sa.call_block_time') }}
    {% endif %}
  GROUP BY i.token_account, i.account_mint, sa.call_block_time
),

latest_init_before_closure AS (
  SELECT
    i.token_account,
    i.account_mint,
    c.call_block_time AS closure_time,
    MAX(i.event_time) AS latest_init_time
  FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }} c
  -- Join to ALL initializations, not just recent ones
  LEFT JOIN {{ ref('alt_solana_utils_token_account_initializations') }} i 
    ON c.account_account = i.token_account
    AND i.event_time < c.call_block_time
  WHERE c.call_block_time >= timestamp '{{start_date}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('c.call_block_time') }}
    {% endif %}
  GROUP BY i.token_account, i.account_mint, c.call_block_time
),

ownership_transfers AS (
  SELECT
    sa.account_owned AS token_account,
    sa.newAuthority AS account_owner,
    li.account_mint, -- get mint from latest initialization
    sa.call_block_time AS event_time,
    'owner_change' AS event_type,
    SUBSTRING(sa.account_owned, 1, 1) AS token_account_prefix
  FROM {{ source('spl_token_solana', 'spl_token_call_setauthority') }} sa
  LEFT JOIN latest_init_before_transfer li 
    ON sa.account_owned = li.token_account
    AND sa.call_block_time = li.transfer_time
  WHERE json_extract_scalar(sa.authorityType, '$.AuthorityType') = 'AccountOwner'
    AND sa.call_block_time >= timestamp '{{start_date}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('sa.call_block_time') }}
    {% endif %}
),

closures AS (
  SELECT
    c.account_account AS token_account,
    NULL AS account_owner,
    li.account_mint, -- get mint from latest initialization
    c.call_block_time AS event_time,
    'close' AS event_type,
    SUBSTRING(c.account_account, 1, 1) AS token_account_prefix
  FROM {{ source('spl_token_solana', 'spl_token_call_closeaccount') }} c
  LEFT JOIN latest_init_before_closure li 
    ON c.account_account = li.token_account
    AND c.call_block_time = li.closure_time
  WHERE c.call_block_time >= timestamp '{{start_date}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('c.call_block_time') }}
    {% endif %}
)

-- Only query recent initialization events in incremental mode
{% if is_incremental() %}
SELECT * FROM initializations
WHERE {{ incremental_predicate('event_time') }}
{% else %}
SELECT * FROM initializations
{% endif %}

UNION ALL
SELECT * FROM ownership_transfers
UNION ALL
SELECT * FROM closures 