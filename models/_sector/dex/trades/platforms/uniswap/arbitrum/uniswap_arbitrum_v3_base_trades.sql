{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v3_arbitrum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2021-06-01' %}

{{
    uniswap_v3_forked_base_trades(
        blockchain = 'arbitrum'
        , project = 'uniswap'
        , version = '3'
        , Pair_evt_Swap = source('uniswap_v3_arbitrum', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_arbitrum', 'Factory_evt_PoolCreated')
    )
}}