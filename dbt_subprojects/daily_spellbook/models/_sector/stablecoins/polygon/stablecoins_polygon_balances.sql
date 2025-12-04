{{
  config(
    schema = 'stablecoins_polygon',
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
    blockchain = 'polygon',
    start_date = '2023-08-01',
  )
}}
 