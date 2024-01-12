{{
    config(
        schema = 'dodo_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{%
    set config_other_sources = [
        {'version': '2_dpp', 'source': 'DPPOracle_evt_DODOSwap'},
    ]
%}

{{
    dodo_compatible_trades(
        blockchain = 'base',
        project = 'dodo',
        other_sources = config_other_sources
    )
}}
