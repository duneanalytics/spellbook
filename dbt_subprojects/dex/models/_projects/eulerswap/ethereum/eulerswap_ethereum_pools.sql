{{ config(
    schema = 'eulerswap_ethereum'
    , alias = 'pools'
    , materialized = 'view'
    )
}}

{{
    eulerswap_compatible_pools(
          blockchain = 'ethereum'
        , project = 'eulerswap'
        , version = '1'
        , PoolCreations = ref('eulerswap_ethereum_pool_creations')
    )
}}