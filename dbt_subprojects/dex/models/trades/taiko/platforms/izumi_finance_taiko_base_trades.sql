{{
    config(
        schema = 'izumi_finance_taiko',
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
        blockchain = 'taiko',
        project = 'izumi',
        version = '1',
        Pair_evt_Swap = ref('stg_izumi_finance_taiko_swaps'),
        Factory_evt_PoolCreated = ref('stg_izumi_finance_taiko_pool'),
        taker_column_name = 'evt_tx_to'
    )
}}