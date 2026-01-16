{% set chain = 'arbitrum' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_balances_enriched_test',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- extended balances enriched test: enriched with token metadata and usd prices

{{
  balances_incremental_subset_daily_enrich(
    base_balances = ref('stablecoins_' ~ chain ~ '_extended_balances_test')
  )
}}
