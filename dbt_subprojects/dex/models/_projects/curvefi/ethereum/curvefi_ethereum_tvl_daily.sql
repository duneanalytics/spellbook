{{
  config(
    schema = 'curvefi_ethereum',
    alias = 'tvl_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['id', 'day', 'blockchain', 'version'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

{{
    enrich_dex_pools_balances(
          dex_pools_balances = ref('curvefi_ethereum_pools_balances')
        , pools_table = ref('curve_ethereum_view_pools')
        , token0 = 'coin0'
        , token1 = 'coin1'
        , token2 = 'coin2'
        , token3 = 'coin3'
        , pools_column = 'pool_address'
        , blockchain = 'ethereum'
        , project = 'curvefi'
    )
}}