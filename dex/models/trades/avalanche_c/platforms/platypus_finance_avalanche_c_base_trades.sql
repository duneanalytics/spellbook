{{
    config(
        schema = 'platypus_finance_avalanche_c',
        alias ='base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{%
    set config_sources = [
        {'version': '1', 'source': 'Pool_evt_Swap'},
    ]
%}

{{
    generic_spot_v2_compatible_trades(
        blockchain = 'avalanche_c',
        project = 'platypus_finance',
        sources = config_sources
    )
}}
