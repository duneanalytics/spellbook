{{
    config(
        schema = 'dodo_optimism',
        alias = 'optimism_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{%
    set config_other_sources = [
        {'version': '2', 'source': 'DVM_evt_DODOSwap'},
        {'version': '2', 'source': 'DPP_evt_DODOSwap'},
        {'version': '2', 'source': 'DSP_evt_DODOSwap'},
    ]
%}

{{
    dodo_compatible_trades(
        blockchain = 'optimism',
        project = 'dodo',
        other_sources = config_other_sources
    )
}}
