{{
    config(
        schema = 'wombat_bnb',
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
        {'version': '1', 'source': 'Pool_evt_Swap'},
        {'version': '1', 'source': 'HighCovRatioFeePool_evt_Swap'},
        {'version': '1', 'source': 'DynamicPool_evt_Swap'},
        {'version': '1', 'source': 'mWOM_Pool_evt_Swap'},
        {'version': '1', 'source': 'qWOM_WOMPool_evt_Swap'},
        {'version': '1', 'source': 'WMX_WOM_Pool_evt_Swap'},
    ]
%}

{{
    generic_spot_v2_compatible_trades(
        blockchain = 'bnb',
        project = 'wombat',
        sources = config_sources
    )
}}
