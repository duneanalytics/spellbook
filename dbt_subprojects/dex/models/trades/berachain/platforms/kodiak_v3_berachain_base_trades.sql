{{ config(
    schema = 'kodiak_v3_berachain'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'berachain'
        , project = 'kodiak'
        , version = '3'
        , Pair_evt_Swap = source('kodiak_berachain', 'v3pair_evt_swap')
        , Factory_evt_PoolCreated = source('kodiak_berachain', 'factoryv3_evt_poolcreated')
        , optional_columns = ['f.fee', 'f.tickSpacing', 't.sqrtPriceX96', 't.liquidity', 't.tick']
    )
}}
