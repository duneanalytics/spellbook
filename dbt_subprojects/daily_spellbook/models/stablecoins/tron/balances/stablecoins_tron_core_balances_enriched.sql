{% set chain = 'tron' %}

{{
  config(
    tags = ['stablecoins'],
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

{{
  stablecoins_tron_balances_from_transfers_enrich(
    base_balances = ref('stablecoins_' ~ chain ~ '_core_balances'),
    chain = chain,
    token_list = 'core'
  )
}}
