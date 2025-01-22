{{
    config(
        schema = 'woofi_bnb',
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
            'exclude': '0xcef5be73ae943b77f9bc08859367d923c030a269, 0x114f84658c99aa6ea62e3160a87a16deaf7efe83'
        },
        {
            'version': '1',
            'source': 'WooRouter_evt_WooRouterSwap'
        },
        {
            'version': '2',
            'source': 'WooRouterV2_evt_WooRouterSwap'
        },
    ]
%}

{{
    generic_spot_v2_compatible_trades(
        blockchain = 'bnb',
        project = 'woofi',
        sources = config_sources,
        maker = '"from"'
    )
}}
