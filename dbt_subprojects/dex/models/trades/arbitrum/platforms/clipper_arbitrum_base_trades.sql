{{
    config(
        schema = 'clipper_arbitrum',
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
        {'version': 'coves2', 'source': 'ClipperCove_v2_evt_CoveSwapped'},
        {'version': '1', 'source': 'ClipperPackedVerifiedExchange_evt_Swapped'},
        {'version': '2', 'source': 'ClipperPackedVerifiedExchange_v2_evt_Swapped'},
    ]
%}

{{
    clipper_compatible_trades(
        blockchain = 'arbitrum',
        project = 'clipper',
        sources = config_sources
    )
}}
