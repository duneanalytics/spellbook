{{
  config(
    schema='solana_utils'
    , alias='spl_token_accounts_state_history'
    , partition_by=['address_prefix']
    , materialized='table'
    , file_format='delta'
    , unique_key=['address', 'address_prefix', 'unique_instruction_key']
  )
}}

with full_history as (
    select
        block_date
        , block_time
        , address
        , address_prefix
        , event_type
        , token_balance_owner
        , token_mint_address
        , unique_instruction_key
        , block_slot
        , tx_index
        , outer_instruction_index
        , inner_instruction_index
    from {{ ref('solana_utils_spl_token_accounts_raw') }}
)
, ranked_src as (
    select
        address_prefix
        , address
        , event_type
        , token_balance_owner
        , token_mint_address -- Keep original mint for later logic
        , max(case when token_mint_address is not null then token_mint_address end)
            over (
                partition by address_prefix, address
                order by unique_instruction_key asc
                rows between unbounded preceding and current row
        ) as last_non_null_token_mint_address -- get the latest non-null token_mint_address up to this point
        , block_date as valid_from_block_date
        , block_time as valid_from_block_time
        , unique_instruction_key as valid_from_unique_instruction_key
        , lead(block_date) over (
            partition by address_prefix, address
            order by unique_instruction_key asc
        ) as valid_to_block_date
        , lead(block_time) over (
            partition by address_prefix, address
            order by unique_instruction_key asc
        ) as valid_to_block_time
        , lead(unique_instruction_key) over (
            partition by address_prefix, address
            order by unique_instruction_key asc
        ) as valid_to_unique_instruction_key
        , row_number() over (
            partition by address_prefix, address
            order by unique_instruction_key desc
        ) as rn
    from full_history
)
select
    address_prefix
    , address
    , event_type
    , token_balance_owner
    , CASE
        WHEN event_type = 'owner_change' THEN last_non_null_token_mint_address
        ELSE token_mint_address
    END AS token_mint_address
    , valid_from_block_date
    , valid_from_block_time
    , valid_from_unique_instruction_key
    , coalesce(valid_to_block_date, date '9999-12-31') as valid_to_block_date
    , coalesce(valid_to_block_time, timestamp '9999-12-31 23:59:59') as valid_to_block_time
    , coalesce(valid_to_unique_instruction_key, '999999999-999999-9999-9999') as valid_to_unique_instruction_key
    , if(rn = 1, 1, 0) as is_active
from ranked_src