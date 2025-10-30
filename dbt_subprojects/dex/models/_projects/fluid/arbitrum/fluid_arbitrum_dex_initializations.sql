{{ config(
    schema = 'fluid_arbitrum'
    , alias = 'dex_initializations'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['dex']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    fluid_liquidity_pools_initializations(
        blockchain = 'arbitrum'
        , project = 'fluid'
        , version = '1'
        , liquidity_pools = ref('fluid_arbitrum_pools')
    )
}}
