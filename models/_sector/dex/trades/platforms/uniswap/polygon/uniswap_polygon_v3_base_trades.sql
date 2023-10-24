{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v3_polygon',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2021-12-20' %}

{{
    uniswap_v3_forked_base_trades(
        Pair_evt_Swap = source('uniswap_v3_polygon', 'UniswapV3Pool_evt_Swap')
        , Factory_evt_PairCreated = source('uniswap_v3_polygon', 'Factory_evt_PoolCreated')
    )
}}