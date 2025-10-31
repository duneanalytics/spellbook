{{
    config(
        schema = 'izumi_finance_v2_plume',
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
        blockchain = 'plume',
        project = 'izumi_finance',
        version = '3',
        Pair_evt_Swap = source('izumi_finance_plume', 'iziswapclassicpair_evt_iziv2swap'),
        Factory_evt_PairCreated = source('izumi_finance_plume', 'iziswapclassicfactory_evt_paircreated')
    )
}}