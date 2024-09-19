{{
    config(
        schema = 'fusionx_mantle',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'mantle'
        , project = 'fusionx'
        , version = '3'
        , Pair_evt_Swap = source('fusionx_mantle', 'FusionXV3Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('fusionx_mantle', 'FusionXV3Factory_evt_PoolCreated')
    )
}}