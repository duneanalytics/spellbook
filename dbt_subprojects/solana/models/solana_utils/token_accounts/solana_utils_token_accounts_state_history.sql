{{
  config(
    schema='solana_utils'
    , alias='token_accounts_state_history'
    , partition_by=['valid_from_block_month', 'address_prefix']
    , materialized='table'
    , file_format='delta'
    , unique_key=['valid_from_block_month', 'address', 'address_prefix', 'unique_instruction_key']
  )
}}

select
    address_prefix
    , address
    , event_type
    , token_balance_owner
    , token_mint_address
    , valid_from_block_month
    , valid_from_block_date
    , valid_from_block_time
    , valid_from_unique_instruction_key
    , valid_to_block_month
    , valid_to_block_date
    , valid_to_block_time
    , valid_to_unique_instruction_key
    , is_active
from
    {{ ref('solana_utils_spl_token_accounts_state_history')}}
union all
select
    address_prefix
    , address
    , event_type
    , token_balance_owner
    , token_mint_address
    , valid_from_block_month
    , valid_from_block_date
    , valid_from_block_time
    , valid_from_unique_instruction_key
    , valid_to_block_month
    , valid_to_block_date
    , valid_to_block_time
    , valid_to_unique_instruction_key
    , is_active
from
    {{ ref('solana_utils_spl_token_2022_accounts_state_history')}}