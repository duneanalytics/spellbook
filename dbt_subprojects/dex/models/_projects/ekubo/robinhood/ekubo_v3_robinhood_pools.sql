{{ config(
    schema = 'ekubo_v3_robinhood'
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
          blockchain = 'robinhood'
        , project = 'ekubo'
        , version = '3'
        , pool_init = source('ekubo_v3_robinhood', 'core_evt_poolinitialized')
        , weth_address = '0x0bd7d308f8e1639fab988df18a8011f41eacad73'
    )
}}
