{{
  config(
    schema='solana_utils'
    , alias='token_accounts'
    , materialized='view'
  )
}}

select
    *
from
    {{ ref('solana_utils_token_accounts_state_history')}}
where
    is_active = 1