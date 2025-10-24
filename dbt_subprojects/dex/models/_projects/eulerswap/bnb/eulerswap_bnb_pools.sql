{{ config(
    schema = 'eulerswap_bnb'
    , alias = 'pools'
    , materialized = 'view'
    )
}}

{{
    eulerswap_compatible_pools(
          blockchain = 'bnb'
        , project = 'eulerswap'
        , version = '1'
        , PoolCreations = ref('eulerswap_bnb_pool_creations')
    )
}}