{{
    config(
        schema = 'clipper_polygon',
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
        {'version': 'coves1', 'source': 'ClipperCove_evt_CoveSwapped'},
        {'version': '1', 'source': 'ClipperDirectExchange_evt_Swapped'},
        {'version': '2', 'source': 'ClipperVerifiedExchange_evt_Swapped'},
    ]
%}

{{
    clipper_compatible_trades(
        blockchain = 'polygon',
        project = 'clipper',
        sources = config_sources
    )
}}
