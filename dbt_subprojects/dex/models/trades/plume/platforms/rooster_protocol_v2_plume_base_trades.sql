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
    uniswap_compatible_v3_trades(
        blockchain = 'plume',
        project = 'rooster_protocol',
        version = '2',
        Pair_evt_Swap = source('rooster_protocol_plume', 'algebrapool_evt_swap'),
        Factory_evt_PoolCreated = source('rooster_protocol_plume', 'algebrafactory_evt_pool')
        ,optional_columns = []
    )
}}
