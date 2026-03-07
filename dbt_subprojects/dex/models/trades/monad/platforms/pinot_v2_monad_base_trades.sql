{{
    config(
        schema = 'pinot_v2_monad_base_trades',
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
        blockchain = 'monad',
        project = 'pinot',
        version = '2',
        Pair_evt_Swap = source('pinot_monad', 'uniswapv2pair_evt_swap'),
        Factory_evt_PairCreated = source('pinot_monad', 'uniswapv2factory_evt_paircreated')
    )
}}
