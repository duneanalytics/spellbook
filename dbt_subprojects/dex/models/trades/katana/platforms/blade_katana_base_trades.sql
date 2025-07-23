{{
    config(
        schema = 'blade_katana',
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
        {'version': '1', 'source': 'BladeVerifiedExchange_evt_Swapped'},
    ]
%}

{{
    blade_compatible_trades(
        blockchain = 'katana',
        project = 'sushiswap',
        sources = config_sources
    )
}}
