{{ config(
    schema = 'fluid_polygon'
    , alias = 'dex_fee_updates'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['dex', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    fluid_liquidity_pools_fee_updates(
        blockchain = 'polygon'
        , project = 'fluid'
        , version = '1'
        , liquidity_pools = ref('fluid_polygon_pools')
    )
}}
