{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v3_celo',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2022-07-07' %}

{{
    uniswap_v3_forked_base_trades(
        Pair_evt_Swap = source('uniswap_v3_celo', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_celo', 'UniswapV3Factory_evt_PoolCreated')
    )
}}