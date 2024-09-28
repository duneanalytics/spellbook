{{ config(
    schema = 'woofi_optimism'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{%
    set config_sources = [
        {
            'version': '1',
            'source': 'WooPPV2_evt_WooSwap',
            'exclude': '0xeaf1ac8e89ea0ae13e0f03634a4ff23502527024'
        },
        {
            'version': '1',
            'source': 'WooRouterV2_evt_WooRouterSwap'
        },
    ]
%}

{{
    generic_spot_v2_compatible_trades(
        blockchain = 'optimism',
        project = 'woofi',
        sources = config_sources,
        maker = '"from"'
    )
}}
