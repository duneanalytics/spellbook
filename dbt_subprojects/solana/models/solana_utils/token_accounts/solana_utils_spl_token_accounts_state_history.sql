{{
  config(
    schema='solana_utils'
    , alias='spl_token_accounts_state_history'
    , partition_by=['address_prefix']
    , materialized='table'
    , file_format='delta'
    , unique_key=['address', 'address_prefix', 'valid_from_unique_instruction_key']
  )
}}

with ranked_src as (
    select
        address_prefix
        , address
        , event_type
        , token_balance_owner
        , token_mint_address -- Keep original mint for later logic
        , max(case when token_mint_address is not null then token_mint_address end)
            over (
                partition by address
                order by unique_instruction_key asc
                rows between unbounded preceding and current row
        ) as last_non_null_token_mint_address -- get the latest non-null token_mint_address up to this point
        , block_date
        , unique_instruction_key as valid_from_unique_instruction_key
        , lead(unique_instruction_key) over (
            partition by address
            order by unique_instruction_key asc
        ) as valid_to_unique_instruction_key
        , row_number() over (
            partition by address
            order by unique_instruction_key desc
        ) as rn
    from {{ ref('solana_utils_spl_token_accounts_raw') }}
)
, base_state as ( -- Calculate the definitive token_mint_address first
    select
        ranked_src.address_prefix
        , ranked_src.address
        , ranked_src.event_type
        , ranked_src.token_balance_owner
        , CASE
            WHEN ranked_src.event_type = 'owner_change' THEN ranked_src.last_non_null_token_mint_address
            ELSE ranked_src.token_mint_address
        END AS token_mint_address
        , ranked_src.block_date
        , ranked_src.valid_from_unique_instruction_key
        , ranked_src.valid_to_unique_instruction_key
        , ranked_src.rn
        , ranked_src.last_non_null_token_mint_address -- Keep for debugging if needed, or remove
    from ranked_src
)
, final_state_calc as ( -- Simplified CTE without NFT join
    select
        base_state.address_prefix
        , base_state.address
        , base_state.event_type
        , base_state.token_balance_owner
        , base_state.token_mint_address -- Use pre-calculated address
        , base_state.block_date
        , base_state.valid_from_unique_instruction_key
        , coalesce(base_state.valid_to_unique_instruction_key, '999999999-999999-9999-9999') as valid_to_unique_instruction_key
        , if(base_state.rn = 1, 1, 0) as is_active
    from base_state
)


select
    address_prefix
    , address
    , event_type
    , token_balance_owner
    , token_mint_address
    , block_date
    , valid_from_unique_instruction_key
    , valid_to_unique_instruction_key
    , is_active
from final_state_calc