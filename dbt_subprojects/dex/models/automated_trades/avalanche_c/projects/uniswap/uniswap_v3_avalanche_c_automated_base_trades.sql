{{ config(
        schema = 'uniswap_v3_avalanche_c',
        alias = 'automated_base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{
    uniswap_v3_forks_trades(
        blockchain = 'avalanche_c'
        , version = '3'
        , project = 'null'
        , Pair_evt_Swap = ref('uniswap_v3_avalanche_c_decoded_pool_evt_swap')
        , Factory_evt_PoolCreated = ref('uniswap_v3_avalanche_c_decoded_factory_evt')
    )
}}