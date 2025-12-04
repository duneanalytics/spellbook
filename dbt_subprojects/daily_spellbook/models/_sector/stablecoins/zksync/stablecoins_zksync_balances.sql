{{
  config(
    tags = ['prod_exclude'],
    schema = 'stablecoins_zksync',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address', 'blockchain'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

{{
  stablecoins_balances(
    blockchain = 'zksync',
    start_date = '2023-03-24'
  )
}}
