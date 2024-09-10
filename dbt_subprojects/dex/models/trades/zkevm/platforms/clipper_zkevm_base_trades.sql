{{
    config(
        schema = 'clipper_zkevm',
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
        {'version': '1', 'source': 'ClipperPackedOracleVerifiedExchange_evt_Swapped'},
    ]
%}

{{
    clipper_compatible_trades(
        blockchain = 'zkevm',
        project = 'clipper',
        sources = config_sources
    )
}}