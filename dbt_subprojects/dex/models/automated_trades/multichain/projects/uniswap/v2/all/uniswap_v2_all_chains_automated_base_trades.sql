{{ config(
        schema = 'uniswap_v2_decoded_events',
        alias = 'all_chains_automated_base_trades',
        partition_by = ['block_month', 'blockchain'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_number', 'tx_index', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{{
    uniswap_v2_forks_trades(
        version = '2'
        , Pair_evt_Swap = ref('uniswap_v2_all_chains_decoded_pool_evt_swap')
        , Factory_evt_PairCreated = ref('uniswap_v2_all_chains_decoded_factory_evt')
    )
}}