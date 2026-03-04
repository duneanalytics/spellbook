{% set chain = 'shape' %}

{{
  config(
    tags = ['prod_exclude', 'stablecoins'],
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

-- core balances: tracks balances (from transfers) for stablecoins in the frozen core list

{{ stablecoins_balances_from_transfers(
    transfers = source('stablecoins_' ~ chain, 'core_transfers'),
    start_date = '2024-08-06'
) }}
