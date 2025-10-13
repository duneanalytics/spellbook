{{ config(
    schema = 'eulerswap_arbitrum'
    , alias = 'pools'
    , materialized = 'view'
    )
}}

{{
    eulerswap_compatible_pools(
          blockchain = 'arbitrum'
        , project = 'eulerswap'
        , version = '1'
        , PoolCreations = ref('eulerswap_arbitrum_pool_creations')
    )
}}