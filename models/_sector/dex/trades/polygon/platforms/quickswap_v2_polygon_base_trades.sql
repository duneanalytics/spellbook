{{
    config(
        schema = 'quickswap_v2_polygon',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{%
    set config_sources = [
        {'version': 'quickswap_v3_polygon', 'source': 'AlgebraPool_evt_Swap'},
        {'version': 'quickswap_v3_polygon', 'source': 'AlgebraFactory_evt_Pool'}
    ]
%}

{{
    quickswap_compatible_trades(
        blockchain = 'polygon',
        project = 'quickswap',
        version = '2',
        sources = config_sources
    )
}}
