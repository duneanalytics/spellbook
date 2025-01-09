{{
    config(
        schema = 'zkswap_finance_v3_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'zksync',
        project = 'zkswap_finance',
        version = '3',
        Pair_evt_Swap = source('zkswap_finance_v3_zksync', 'ZFV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('zkswap_finance_v3_zksync', 'ZFV3Factory_evt_PoolCreated')
    )
}}
