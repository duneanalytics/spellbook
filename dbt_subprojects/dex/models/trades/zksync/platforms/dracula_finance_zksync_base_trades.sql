{{
    config(
        schema = 'dracula_finance_zksync',
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
        blockchain = 'zksync',
        project = 'dracula_finance',
        version = '1',
        Pair_evt_Swap = source('dracula_finance_zksync', 'DraculaPair_evt_Swap'),
        Factory_evt_PairCreated = source('dracula_finance_zksync', 'DraculaFactory_evt_PairCreated')
    )
}}
