{{
    config(
        schema = 'wombat_exchange_arbitrum',
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
        {'version': '2', 'source': 'CrossChainPool_evt_SwapV2'},
    ]
%}

{{
    generic_spot_v2_compatible_trades(
        blockchain = 'arbitrum',
        project = 'wombatexchange',
        sources = config_sources
    )
}}