{{ config(
    schema = 'eulerswap_unichain'
    , alias = 'pools'
    , materialized = 'view'
    )
}}

{{
    eulerswap_compatible_pools(
          blockchain = 'unichain'
        , project = 'eulerswap'
        , version = '1'
        , PoolCreations = ref('eulerswap_unichain_pool_creations')
    )
}}