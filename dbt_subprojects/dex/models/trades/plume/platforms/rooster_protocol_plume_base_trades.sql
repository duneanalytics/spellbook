{{
    config(
        schema = 'rooster_protocol_plume',
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
        version = '2',
        source_evt_swap = source('rooster_protocol_plume', 'maverickv2pool_evt_poolswap'),
        source_evt_pool = source('rooster_protocol_plume', 'maverickv2factory_evt_poolcreated')
    )
}}
