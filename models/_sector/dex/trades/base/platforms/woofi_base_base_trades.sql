{{
    config(
        schema = 'woofi_base',
        alias ='base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


{{
    woofi_compatible_trades(
        blockchain = 'base',
        project = 'woofi',
        version = '2',
        Pair_evt_Swap = source('woofi_base', 'WooPPV2_evt_WooSwap'),
    )
}}
