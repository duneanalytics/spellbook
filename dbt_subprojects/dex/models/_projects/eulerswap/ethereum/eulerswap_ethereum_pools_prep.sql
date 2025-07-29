{{ config(
    schema = 'eulerswap_ethereum'
    , alias = 'pools_prep'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['pool', 'blockchain']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

{{
    eulerswap_compatible_pools_prep(
          blockchain = 'ethereum'
        , project = 'eulerswap'
        , version = '1'
        , PoolCreations = ref('eulerswap_ethereum_pool_creations')
    )
}}