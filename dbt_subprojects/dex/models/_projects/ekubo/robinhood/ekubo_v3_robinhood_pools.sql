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
        , pool_init = ekubo_v3_core_events_from_logs(
              blockchain = 'robinhood'
            , ekubo_core_contract = '0x00000000000014aA86C5d3c41765bb24e11bd701'
            , start_block_number = '33534'
            , event = 'PoolInitialized'
          )
        , weth_address = '0x0bd7d308f8e1639fab988df18a8011f41eacad73'
    )
}}
