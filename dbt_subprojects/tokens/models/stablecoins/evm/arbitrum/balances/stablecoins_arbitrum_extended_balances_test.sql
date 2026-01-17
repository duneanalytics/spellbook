{% set chain = 'arbitrum' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_balances_test',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- extended balances test: tracks balances for newly added stablecoins (from transfers)

{{ stablecoins_balances_from_transfers(
    transfers = ref('stablecoins_' ~ chain ~ '_extended_transfers'),
    start_date = '2025-01-01'
) }}
