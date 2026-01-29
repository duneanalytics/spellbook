{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_balances_enriched',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- core balances enriched with usd prices

{{ stablecoins_svm_balances_enrich(
  base_balances = ref('stablecoins_' ~ chain ~ '_core_balances'),
  blockchain = chain
) }}
