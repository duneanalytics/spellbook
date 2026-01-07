 {{
  config(
        schema = 'solana_utils',
        alias = 'token_accounts',
        materialized = 'view',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "solana_utils",
                                    \'["ilemi"]\') }}')
}}


-- backwards compatible view
-- the preferred model is the state_history table for full correctness
select
      address
      ,address_prefix
      ,token_mint_address
      ,token_balance_owner
      ,token_version as account_type
from {{ source('token_accounts_solana','state_history') }}
where is_active = true