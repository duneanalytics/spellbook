{{
  config(
    schema = 'curvefi_ethereum',
    alias = 'pools_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address', 'token_address', 'day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

{{
    dex_pools_balances(
          blockchain = 'ethereum'
        , start_date = '2022-06-01'
        , pools_table = ref('curve_ethereum_view_pools')
        , pools_column = 'pool_address'
    )
}}