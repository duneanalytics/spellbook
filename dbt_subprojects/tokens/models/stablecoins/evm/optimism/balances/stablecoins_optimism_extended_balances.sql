{% set chain = 'optimism' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- extended balances: tracks balances for newly added stablecoins (not in core list)

with

stablecoin_tokens as (
  select contract_address as token_address
  from {{ ref('tokens_' ~ chain ~ '_erc20_stablecoins_extended') }}
),

-- note: update start_date when adding new stablecoins
balances as (
  {{
    balances_incremental_subset_daily(
        blockchain = chain,
        token_list = 'stablecoin_tokens',
        start_date = '2025-01-01'
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
