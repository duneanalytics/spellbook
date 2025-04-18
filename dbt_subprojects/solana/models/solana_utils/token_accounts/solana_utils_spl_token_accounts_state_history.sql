{{
  config(
    schema='solana_utils'
    , alias='spl_token_accounts_state_history'
    , partition_by=['block_date']
    , materialized='table'
    , file_format='delta'
    , unique_key=['block_date', 'token_account', 'token_account_prefix', 'unique_instruction_key']
  )
}}

with full_history as (
    select
        token_account_prefix
        , token_account
        , event_type
        , account_owner
        , account_mint
        , block_date
        , unique_instruction_key
    from {{ ref('solana_utils_spl_token_accounts_raw') }}
)
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
        , block_date
        , unique_instruction_key as valid_from_unique_instruction_key
        , lead(unique_instruction_key) over (
            partition by token_account_prefix, token_account
            order by unique_instruction_key asc
        ) as valid_to_unique_instruction_key
        , row_number() over (
            partition by token_account_prefix, token_account
            order by unique_instruction_key desc
        ) as rn
    from full_history
)
select
    token_account_prefix
    , token_account
    , event_type
    , account_owner as token_balance_owner
    , CASE
        WHEN event_type = 'owner_change' THEN last_non_null_account_mint
        ELSE account_mint
    END AS token_mint_address
    , block_date
    , valid_from_unique_instruction_key
    , coalesce(valid_to_unique_instruction_key, '999999999-999999-9999-9999') as valid_to_unique_instruction_key
    , if(rn = 1, 1, 0) as is_active
from ranked_src