{{
    config(
        schema = 'clipper_base',
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
        {'version': '1', 'source': 'ClipperPackedVerifiedExchange_evt_Swapped'},
    ]
%}

{{
    clipper_compatible_trades(
        blockchain = 'base',
        project = 'clipper',
        sources = config_sources
    )
}}
