{{
  config(
    schema='solana_utils'
    , alias='spl_token_2022_accounts'
    , partition_by=['token_account_prefix']
    , materialized='table'
    , file_format='delta'
    , unique_key=['token_account_prefix', 'token_account', 'unique_instruction_key']
  )
}}

with nft as (
    select distinct
      account_mint
    from
      {{ ref('tokens_solana_nft')}}
    where
      account_mint is not null
)
select
    t22.token_account_prefix
    , t22.token_account
    , t22.event_type
    , t22.token_balance_owner
    , t22.token_mint_address
    , t22.block_date
    , t22.valid_from_unique_instruction_key
    , t22.valid_to_unique_instruction_key
    , t22.is_active
    , case when nft.account_mint is not null
      then 'nft'
      else 'fungible'
    end as account_type
from
  {{ ref('solana_utils_spl_token_2022_accounts_state_history')}} as t22
left join
  nft 
  on t22.token_mint_address = nft.account_mint