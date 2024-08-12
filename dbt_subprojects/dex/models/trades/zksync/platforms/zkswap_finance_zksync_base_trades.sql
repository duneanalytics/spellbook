{{
    config(
        schema = 'zkswap_finance_zksync',
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
        project = 'zkswap_finance',
        version = '1',
        Pair_evt_Swap = source('zkswap_finance_zksync', 'ZFPair_evt_Swap'),
        Factory_evt_PairCreated = source('zkswap_finance_zksync', 'Factory_evt_PairCreated')
    )
}}
