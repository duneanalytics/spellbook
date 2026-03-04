{{
    config(
        schema = 'supernova_v2_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v2_trades(
        blockchain = 'ethereum',
        project = 'supernova',
        version = '2',
        Pair_evt_Swap = source('supernova_ethereum', 'pair_evt_swap'),
        Factory_evt_PairCreated = source('supernova_ethereum', 'pairgenerator_evt_paircreated')
    )
}}
