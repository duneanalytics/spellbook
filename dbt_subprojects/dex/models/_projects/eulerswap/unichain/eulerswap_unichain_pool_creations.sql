{{ config(
    schema = 'eulerswap_unichain'
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
          blockchain = 'unichain'
        , project = 'eulerswap'
        , version = '1'
        , uniswap_pools = ref('uniswap_v4_unichain_pools')
        , factory_univ4_pooldeployed  = source('eulerswap_unichain', 'eulerswapfactory_uniswapv4_evt_pooldeployed')
        , factory_univ4_poolconfig = source('eulerswap_unichain', 'eulerswapfactory_uniswapv4_evt_poolconfig')
    )
}}