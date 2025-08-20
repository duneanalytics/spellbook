{{ config(
    schema = 'eulerswap_ethereum'
    , alias = 'pool_creations'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['pool', 'blockchain']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

{{
    eulerswap_compatible_univ4_pools(
          blockchain = 'ethereum'
        , project = 'eulerswap'
        , version = '1'
        , uniswap_pools = ref('uniswap_v4_ethereum_pools')
        , factory_univ4_pooldeployed  = source('eulerswap_ethereum', 'eulerswapfactory_uniswapv4_evt_pooldeployed')
        , factory_univ4_poolconfig = source('eulerswap_ethereum', 'eulerswapfactory_uniswapv4_evt_poolconfig')
    )
}}