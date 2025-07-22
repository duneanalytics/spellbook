{{
    config(
        schema = 'native_bnb',
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
        {'version': '1', 'source': 'NativeRFQPool_evt_RFQTrade'},
    ]
%}

{{
    native_compatible_trades(
        blockchain = 'bnb',
        project = 'native',
        sources = config_sources
    )
}}
