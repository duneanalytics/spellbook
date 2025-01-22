{{
    config(
        schema = 'woofi_avalanche_c',
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
        {
            'version': '1',
            'source': 'WooPP_evt_WooSwap',
            'exclude': '0x5aa6a4e96a9129562e2fc06660d07feddaaf7854'
        },
        {
            'version': '1',
            'source': 'WooRouterV2_evt_WooRouterSwap'
        },
    ]
%}

{{
    generic_spot_v2_compatible_trades(
        blockchain = 'avalanche_c',
        project = 'woofi',
        sources = config_sources,
        maker = '"from"'
    )
}}
