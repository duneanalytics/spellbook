{{ config(
        schema = 'uniswap_v3_blast',
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
        blockchain = 'blast'
        , version = '3'
        , project = 'null'
        , Pair_evt_Swap = ref('uniswap_v3_blast_decoded_pool_evt_swap')
        , Factory_evt_PoolCreated = ref('uniswap_v3_blast_decoded_factory_evt')
    )
}}