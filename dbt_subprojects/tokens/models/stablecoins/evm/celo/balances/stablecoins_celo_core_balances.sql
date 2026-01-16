{% set chain = 'celo' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- core balances: tracks balances for stablecoins in the frozen core list (from transfers)

{{ stablecoins_balances_from_transfers(
    transfers = ref('stablecoins_' ~ chain ~ '_core_transfers')
) }}
