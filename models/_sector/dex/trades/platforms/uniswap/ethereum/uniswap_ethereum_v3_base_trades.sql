{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v3_ethereum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2021-05-04' %}

{{
    uniswap_v3_forked_base_trades(
        Pair_evt_Swap = source('uniswap_v3_ethereum', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = source('uniswap_v3_ethereum', 'Factory_evt_PoolCreated')
    )
}}