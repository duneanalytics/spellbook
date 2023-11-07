{{ config(
    schema = 'uniswap_v3_base',
    alias = 'stg_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{{
    dex_fork_v3_base_trades(
        blockchain = 'base'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Swap = source('uniswap_v3_base', 'UniswapV3Pool_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_base', 'UniswapV3Factory_evt_PoolCreated')
    )
}}