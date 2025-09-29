{{
  config(
    schema = 'pancakeswap_arbitrum',
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
          blockchain = 'arbitrum'
        , start_date = '2023-06-01'
        , pools_table = ref('pancakeswap_arbitrum_pools')
    )
}}