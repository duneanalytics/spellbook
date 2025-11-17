{{
    config(
        schema = 'kuru_monad',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    generic_spot_compatible_trades(
        blockchain = 'monad',
        project = 'kuru',
        version = '1',
        source_evt_swap = source('kuru_monad_testnet', 'kuruflow_evt_kuruflowswap'),
        taker = 'user'
    )
}}
