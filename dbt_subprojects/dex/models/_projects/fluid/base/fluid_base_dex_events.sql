{{ config(
    schema = 'fluid_base'
    , alias = 'dex_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    fluid_dex_events(
        blockchain = 'base'
        , project = 'fluid'
        , version = '1'
        , liquidity_pools = ref('fluid_base_pools')
    )
}}
