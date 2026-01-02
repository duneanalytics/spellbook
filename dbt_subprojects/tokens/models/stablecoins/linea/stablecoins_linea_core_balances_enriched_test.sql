{% set chain = 'linea' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_balances_enriched_test',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- TEST: core balances enriched using ASOF join implementation
-- compare results with stablecoins_linea_core_balances_enriched

{{
  balances_incremental_subset_daily_enrich(
    base_balances = ref('stablecoins_' ~ chain ~ '_core_balances_test')
  )
}}

