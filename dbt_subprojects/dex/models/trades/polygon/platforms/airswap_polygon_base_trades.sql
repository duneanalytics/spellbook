{{
    config(
        schema = 'airswap_polygon',
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
        {'version': 'swap_v3', 'source': 'Swap_v3_evt_Swap'},
        {'version': 'swap_erc20_v4', 'source': 'SwapERC20_v4_evt_SwapERC20'},
    ]
%}

{{
    airswap_compatible_trades(
        blockchain = 'polygon',
        project = 'airswap',
        sources = config_sources
    )
}}
