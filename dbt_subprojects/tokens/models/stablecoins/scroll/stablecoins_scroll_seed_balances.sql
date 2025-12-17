{% set chain = 'scroll' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'seed_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- seed balances: tracks balances for stablecoins in the frozen seed list

with

stablecoin_tokens as (
  select
    symbol,
    contract_address as token_address
  from {{ ref('tokens_' ~ chain ~ '_erc20_stablecoins_seed') }}
),

balances as (
  {{
    balances_incremental_subset_daily(
        blockchain = chain,
        token_list = 'stablecoin_tokens',
        start_date = '2023-10-01'
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
