{{ config(
    schema = 'ekubo_v3_ethereum'
    , alias = 'pools'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['id']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

{{
    ekubo_compatible_pools(
          blockchain = 'ethereum'
        , project = 'ekubo'
        , version = '3'
        , pool_init = source('ekubo_v3_ethereum', 'core_evt_poolinitialized')
        , weth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    )
}}