{{
  config(
    schema = 'arbitrum',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address', 'blockchain'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with
stablecoin_arbitrum_tokens as (
  select
    symbol,
    contract_address
  from {{ref('tokens_arbitrum_erc20_stablecoins')}}
)

,balances as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'arbitrum',
            token_list = 'stablecoin_arbitrum_tokens',
            start_date = '2021-05-26'
      )
    }}
)

select
    t.symbol
    ,b.*
from balances b
left join stablecoin_arbitrum_tokens t
    on b.token_address = t.token_address
