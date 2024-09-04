{{
  config(
    schema = 'rwa_arbitrum',
    alias = 'dex_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with dex_pools as (
    select
        project,
        version,
        pool as address,
        token_address
    from {{ref('rwa_arbitrum_dex_pools')}}
)

,balances as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'arbitrum',
            address_token_list = 'dex_pools',
            start_date = '2023-11-17',
      )
    }}
)

select
    p.project
    ,p.version
    ,b.*
from balances b
left join dex_pools p
on b.address = p.address
and b.token_address = p.token_address


