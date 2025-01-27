{{ config(
        schema = 'uniswap_v3_all_chains',
        alias = 'automated_base_trades',
        partition_by = ['block_month', 'blockchain'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_number', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{
    uniswap_v3_forks_trades(
        version = '3'
        , Pair_evt_Swap = ref('uniswap_v3_all_chains_decoded_pool_evt_swap')
        , Factory_evt_PoolCreated = ref('uniswap_v3_all_chains_decoded_factory_evt')
    )
}}