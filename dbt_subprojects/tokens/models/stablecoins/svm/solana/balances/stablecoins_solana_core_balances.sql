{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_mint_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- core balances: tracks balances for stablecoins in the frozen core list

{{ stablecoins_svm_balances(
  blockchain = chain,
  token_list = 'core',
  start_date = '2026-01-01' -- TEST -> revert to: '2020-10-02'
) }}
