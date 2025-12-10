{{
  config(
    tags = ['prod_exclude'],
    schema = 'stablecoins_worldchain',
    alias = 'base_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with

stablecoin_tokens as (
  select
    symbol,
    contract_address as token_address
  from {{ source('tokens_worldchain', 'erc20_stablecoins')}}
),

balances as (
  {{
    balances_incremental_subset_daily(
        blockchain = 'worldchain',
        token_list = 'stablecoin_tokens',
        start_date = '2024-06-25'
    )
  }}
)

select
  blockchain,
  day,
  address,
  token_address,
  token_standard,
  token_id,
  balance_raw,
  last_updated
from balances
