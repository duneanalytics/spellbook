{{
  config(
    schema='solana_utils'
    , alias='token_accounts'
    , materialized='view'
    , tags = ['prod_exclude']
  )
}}

select
    *
from
    {{ ref('solana_utils_token_accounts_state_history')}}
where
    is_active = 1