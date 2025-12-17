{% set chain = 'worldchain' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'latest_balances_enriched',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- latest balances enriched with token metadata and usd prices

{{
  balances_incremental_subset_daily_enrich(
    base_balances = ref('stablecoins_' ~ chain ~ '_latest_balances')
  )
}}
