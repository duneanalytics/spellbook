{{ config(
    schema = 'saru_apechain',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
) }}

{{
    saru_compatible_v2_trades(
        blockchain = 'apechain',
        project = 'saru',
        version = '1',
        Pair_evt_Swap = source('saru_apechain', 'sarupair_evt_swap'),
        Pair_call_token0 = source('saru_apechain', 'sarupair_call_token0'),
        Pair_call_token1 = source('saru_apechain', 'sarupair_call_token1')
    )
}}
