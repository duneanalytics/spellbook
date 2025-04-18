{{
  config(
    schema='solana_utils'
    , alias='spl_token_accounts'
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
    t.token_account_prefix
    , t.token_account
    , t.event_type
    , t.token_balance_owner
    , t.token_mint_address
    , t.block_date
    , t.valid_from_unique_instruction_key
    , t.valid_to_unique_instruction_key
    , t.is_active
    , case when nft.account_mint is not null
      then 'nft'
      else 'fungible'
    end as account_type
from
  {{ ref('solana_utils_spl_token_accounts_state_history')}} as t
left join
  nft 
  on t.token_mint_address = nft.account_mint