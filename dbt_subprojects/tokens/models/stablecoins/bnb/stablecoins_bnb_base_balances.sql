{% set chain = 'bnb' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
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
  from {{ source('tokens_' ~ chain, 'erc20_stablecoins') }}
),

balances as (
  {{
    balances_incremental_subset_daily(
        blockchain = chain,
        token_list = 'stablecoin_tokens',
        start_date = '2020-09-01'
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
