{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_balances_lead',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_mint_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- core balances using LEAD/forward-fill pattern (benchmark)

-- TEST -> revert to: '2020-10-02' for production
{{ stablecoins_svm_balances_lead(
  blockchain = chain,
  token_list = 'core',
  start_date = '2025-01-01'
) }}
