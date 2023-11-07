{{ config(
    schema = 'uniswap_v3_polygon',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{{
    dex_fork_v3_base_trades(
        blockchain = 'polygon'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Swap = source('uniswap_v3_polygon', 'UniswapV3Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_polygon', 'Factory_evt_PoolCreated')
    )
}}