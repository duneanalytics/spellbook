{{
    config(
        schema = 'tessera_v_base',
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
        {'version': '1', 'source': 'tesseraswap_evt_tesseratrade'},
    ]
%}

{{
    tessera_v_compatible_trades(
        blockchain = 'base',
        project = 'tessera_v',
        sources = config_sources
    )
}}
