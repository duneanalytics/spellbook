{{ config(
    tags=['dunesql'],
    schema = 'uniswap_v3_optimism',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

-- OVM 1 Launch 06-23-21
{% set project_start_date = '2021-06-23' %}

{{
    uniswap_v3_forked_base_trades(
        Pair_evt_Swap = source('uniswap_v3_optimism', 'Pair_evt_Swap')
        , Factory_evt_PoolCreated = ref('uniswap_optimism_pools')
    )
}}