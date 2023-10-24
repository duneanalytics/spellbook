{{ config(
    tags=['dunesql'],
    schema = 'defiswap_ethereum',
    alias ='base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2020-09-09' %}

{{
    uniswap_v2_forked_base_trades(
        Pair_evt_Swap = source('defiswap_ethereum', 'CroDefiSwapPair_evt_Swap')
        , Factory_evt_PairCreated = source('crodefi_ethereum', 'CroDefiSwapFactory_evt_PairCreated')
    )
}}