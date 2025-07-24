{{
    config(
        schema = 'pharaoh_legacy_avalanche_c',
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
        blockchain = 'avalanche_c',
        project = 'pharaoh',
        version = 'legacy',
        Pair_evt_Swap = source('pharaoh_avalanche_c', 'pair_evt_swap'),
        Factory_evt_PairCreated = source('pharaoh_avalanche_c', 'pairfactory_evt_paircreated')
    )
}}
