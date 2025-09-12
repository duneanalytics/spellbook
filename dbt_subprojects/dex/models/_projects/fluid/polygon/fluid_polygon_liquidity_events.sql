{{ config(
    schema = 'fluid_polygon'
    , alias = 'liquidity_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    fluid_liquidity_events(
        blockchain = 'polygon'
        , project = 'fluid'
        , version = '1'
        , liquidity_pools = ref('fluid_polygon_pools')
        , contract_address = '0x52Aa899454998Be5b000Ad077a46Bbe360F4e497'
        , weth_address = '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270'
    )
}}