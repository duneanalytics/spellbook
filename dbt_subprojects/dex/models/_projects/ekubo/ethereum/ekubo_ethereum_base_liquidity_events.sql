{{ config(
    schema = 'ekubo_ethereum'
    , alias = 'base_liquidity_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index', 'event_type']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    ekubo_compatible_liquidity_events(
          blockchain = 'ethereum'
        , project = 'ekubo'
        , version = '1'
        , start_block_number = '22047273'
        , ekubo_core_contract = '0xe0e0e08A6A4b9Dc7bD67BCB7aadE5cF48157d444'
        , position_updated = source ('ekubo_ethereum', 'ekubo_core_evt_positionupdated')
        , position_fees_collected = source ('ekubo_ethereum', 'ekubo_core_evt_positionfeescollected')
        , liquidity_pools = ref('ekubo_ethereum_pools')
    )
}}