{{
  config(
    schema = 'rwa_arbitrum',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with
rwa_tokens as (
  select
    project,
    token_address
  from {{ref('rwa_arbitrum_tokens')}}
  where type = 'RWA'
)

,balances as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'arbitrum',
            token_list = 'rwa_tokens',
            start_date = '2023-11-17'
      )
    }}
)

select
    t.project
    ,b.*
from balances b
left join rwa_tokens t
    on b.token_address = t.token_address
