{% set chain = 'avalanche_c' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- core balances: tracks balances for stablecoins in the frozen core list

with

stablecoin_tokens as (
  select contract_address as token_address
  from {{ ref('tokens_' ~ chain ~ '_erc20_stablecoins_core') }}
),

balances as (
  {{
    balances_incremental_subset_daily(
        blockchain = chain,
        token_list = 'stablecoin_tokens',
        start_date = '2021-01-27'
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
