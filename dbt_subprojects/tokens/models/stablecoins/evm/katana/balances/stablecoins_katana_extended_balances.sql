{% set chain = 'katana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_balances',
    materialized = 'incremental',
    tags = ['prod_exclude'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- extended balances: tracks balances (from transfers) for newly added stablecoins

{{ stablecoins_balances_from_transfers(
    transfers = ref('stablecoins_' ~ chain ~ '_extended_transfers'),
    start_date = '2026-01-01'
) }}
