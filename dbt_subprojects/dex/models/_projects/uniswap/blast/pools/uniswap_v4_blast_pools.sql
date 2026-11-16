{{ config(
    schema = 'uniswap_v4_blast'
    , alias = 'pools'
    , materialized = 'table'
    , file_format = 'delta'
    , tags=['static']
    , post_hook='{{ hide_spells() }}'
    )
}}

{{
    uniswap_compatible_pools(
          blockchain = 'blast'
        , project = 'uniswap'
        , version = '4'
        , fee_column_name = 'fee'
        , pool_column_name = 'id'
        , token0_column_name = 'currency0'
        , token1_column_name = 'currency1'
        , pool_created_event = source('uniswap_v4_blast', 'PoolManager_evt_Initialize')
        , hooks_column_name = 'hooks'
    )
}}