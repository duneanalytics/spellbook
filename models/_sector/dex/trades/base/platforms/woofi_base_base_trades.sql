{{
    config(
        schema = 'woofi_base',
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
        {
            'version': '2',
            'source': 'WooPPV2_evt_WooSwap'
        }
    ]
%}
{{
    generic_spot_v2_compatible_trades(
        blockchain = 'base',
        project = 'woofi',
        sources = config_sources,
        maker = '"from"'
    )
}}

