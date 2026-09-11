{{ config(
    schema = 'ekubo_v3_robinhood'
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
          blockchain = 'robinhood'
        , project = 'ekubo'
        , version = '3'
        , start_block_number = '33534'
        , ekubo_core_contract = '0x00000000000014aA86C5d3c41765bb24e11bd701'
        , position_updated = source ('ekubo_v3_robinhood', 'core_evt_positionupdated')
        , position_fees_collected = source ('ekubo_v3_robinhood', 'core_evt_positionfeescollected')
        , position_fees_accumulated = source ('ekubo_v3_robinhood', 'core_evt_feesaccumulated')
        , liquidity_pools = ref('ekubo_v3_robinhood_pools')
    )
}}
