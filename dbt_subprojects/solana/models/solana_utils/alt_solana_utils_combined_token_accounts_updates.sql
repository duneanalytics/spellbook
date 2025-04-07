{{
  config(
    schema='solana_utils',
    alias='alt_solana_utils_combined_token_accounts_updates',
    materialized='table',
    partition_by=['token_account_prefix']
  )
}}


with token_accounts_states as (
SELECT
  token_account,
  token_balance_owner,
  token_mint_address,
  event_type,
  valid_from,
  valid_to,
  valid_from_instruction_uniq_id,
  valid_to_instruction_uniq_id,
  token_account_prefix,
  'spl_token' as token_program
FROM {{ ref('alt_solana_utils_token_accounts_updates') }}

UNION ALL

SELECT
  token_account,
  token_balance_owner,
  token_mint_address,
  event_type,
  valid_from,
  valid_to,
  valid_from_instruction_uniq_id,
  valid_to_instruction_uniq_id,
  token_account_prefix,
  'token_2022' as token_program
FROM {{ ref('alt_solana_utils_token_2022_accounts_updates') }} 
)

select 
  token_account,
  token_balance_owner,
  token_mint_address,
  event_type,
  valid_from,
  valid_to,
  valid_from_instruction_uniq_id,
  valid_to_instruction_uniq_id,
  token_account_prefix,
  CASE WHEN nft.account_mint is not null then 'nft'
       else 'fungible'
  end as account_type,
  token_program
FROM token_accounts_states
LEFT JOIN (select * from {{ ref('tokens_solana_nft')}} where account_mint is not null) nft ON token_accounts_states.token_mint_address = nft.account_mint


