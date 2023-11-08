{{ config(
    schema = 'uniswap_v3_ethereum'
    , alias = 'stg_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    )
}}

{{
    uniswap_fork_v3_trades(
        blockchain = 'ethereum'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Swap = source('uniswap_v3_ethereum', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated')
    )
}}