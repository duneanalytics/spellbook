{{
    config(
        schema = 'rooster_protocol_v2_plume',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    maverick_compatible_v2_trades(
        blockchain = 'plume',
        project = 'rooster_protocol',
        version = '3',
        Pair_evt_Swap = source('rooster_protocol_plume', 'algebrapool_evt_swap'),
        Factory_evt_PairCreated = source('rooster_protocol_plume', 'algebrafactory_evt_pool')
    )
}}
