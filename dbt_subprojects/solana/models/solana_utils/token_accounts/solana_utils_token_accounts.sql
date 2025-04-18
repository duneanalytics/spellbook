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
    {{ ref('solana_utils_spl_token_accounts')}}
union all
select
    *
from
    {{ ref('solana_utils_spl_token_2022_accounts')}}