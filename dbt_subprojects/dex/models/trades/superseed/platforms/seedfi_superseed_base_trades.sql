{{
    config(
        schema = 'seedfi_superseed',
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
        blockchain = 'superseed',
        project = 'seedfi',
        version = '1',
        Pair_evt_Swap = source('seedfi_superseed', 'seedfipool_evt_swap'),
        Factory_evt_PairCreated = source('seedfi_superseed', 'seedfifactory_evt_paircreated')
    )
}}
