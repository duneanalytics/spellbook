{{
    config(
        schema = 'clipper_ethereum',
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
        {'version': '1', 'source': 'ClipperExchangeInterface_evt_Swapped'},
        {'version': '2', 'source': 'ClipperCaravelExchange_evt_Swapped'},
        {'version': '3', 'source': 'ClipperVerifiedCaravelExchange_evt_Swapped'},
        {'version': '4', 'source': 'ClipperApproximateCaravelExchange_evt_Swapped'},
    ]
%}

{{
    clipper_compatible_trades(
        blockchain = 'ethereum',
        project = 'clipper',
        sources = config_sources
    )
}}
