{{
    config(
        schema = 'fenix_blast',
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
        blockchain = 'blast',
        project = 'fenix',
        version = '2',
        Pair_evt_Swap = source('fenix_blast', 'Pair_evt_Swap'),
        Factory_evt_PairCreated = source('fenix_blast', 'PairFactoryUpgradeable_evt_PairCreated')
    )
}}
