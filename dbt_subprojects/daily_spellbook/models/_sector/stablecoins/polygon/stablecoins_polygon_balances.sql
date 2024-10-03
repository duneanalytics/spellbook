{{
  config(
    schema = 'stablecoins_polygon',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address', 'blockchain'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with
stablecoin_tokens as (
  select distinct
    symbol,
    contract_address as token_address
  from 
    {{ source('tokens_polygon', 'erc20_stablecoins')}}
)

,balances as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'polygon',
            token_list = 'stablecoin_tokens',
            start_date = '2020-09-06'
      )
    }}
)

select
    t.symbol
    ,b.*
from balances b
left join stablecoin_tokens t
    on b.token_address = t.token_address
 