{{
  config(
    schema='solana_utils'
    , alias='token_accounts_scd2_incr'
    , partition_by=['token_account_prefix']
    , materialized='incremental'
    , file_format='delta'
    , pre_hook = '{{ set_trino_session_property(true, "task_scale_writers_enabled", "OVERWRITE") }}'
  )
}}

-- Step 1: Identify token accounts with recent changes
with recent_token_accounts as (
    select distinct 
        token_account
        , token_account_prefix
    from {{ ref('solana_utils_token_accounts_raw') }}
    {% if is_incremental() -%}
    where {{ incremental_predicate('block_time') }}
    {%- endif %}
)
-- Step 2: Get full history for affected accounts
, full_history as (
    select
        t.token_account_prefix
        , t.token_account
        , t.event_type
        , t.account_owner
        , t.account_mint
        , t.unique_instruction_key
        , t.block_time
    from {{ ref('solana_utils_token_accounts_raw') }} t
    inner join recent_token_accounts r
        on t.token_account = r.token_account
        and t.token_account_prefix = r.token_account_prefix
)
-- Step 3: Apply SCD2 logic
, ranked_src as (
    select
        token_account_prefix
        , token_account
        , event_type
        , account_owner
        , account_mint -- Keep original mint for later logic
        , max(case when account_mint is not null then account_mint end)
            over (
                partition by token_account_prefix, token_account
                order by unique_instruction_key asc
                rows between unbounded preceding and current row
        ) as last_non_null_account_mint -- get the latest non-null account_mint up to this point
        , unique_instruction_key as valid_from_unique_instruction_key
        , lead(unique_instruction_key) over (
            partition by token_account_prefix, token_account
            order by unique_instruction_key asc
        ) as valid_to_unique_instruction_key
        , row_number() over (
            partition by token_account_prefix, token_account
            order by unique_instruction_key desc
        ) as is_current
    from full_history
)
-- Final output with SCD2 logic applied
select
    token_account_prefix
    , token_account
    , event_type
    , account_owner as token_balance_owner
    , CASE
        WHEN event_type = 'owner_change' THEN last_non_null_account_mint
        ELSE account_mint
    END AS token_mint_address
    , valid_from_unique_instruction_key
    , coalesce(valid_to_unique_instruction_key, '999999999-999999-9999-9999') as valid_to_unique_instruction_key
    , if(is_current = 1, 1, 0) as is_current
from ranked_src