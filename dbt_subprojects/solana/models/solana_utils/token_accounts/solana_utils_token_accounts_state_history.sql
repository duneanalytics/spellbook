{{
  config(
    schema='solana_utils'
    , alias='token_accounts_state_history'
    , materialized='view'
  )
}}
/*
select
    *
from
    {{ ref('solana_utils_spl_token_accounts_state_history')}}
union all
*/
select
    *
from
    {{ ref('solana_utils_spl_token_2022_accounts_state_history')}}