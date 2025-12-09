{{
  config(
    tags = ['prod_exclude'],
    schema = 'stablecoins_kaia',
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
  from {{ source('tokens_kaia', 'erc20_stablecoins')}}
),

balances as (
  {{
    balances_incremental_subset_daily_new(
        blockchain = 'kaia',
        token_list = 'stablecoin_tokens',
        start_date = '2023-09-28'
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
