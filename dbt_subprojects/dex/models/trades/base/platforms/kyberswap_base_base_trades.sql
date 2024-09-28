{{
    config(
        schema = 'kyberswap_base',
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
            'version': 'elastic',
            'source_evt_swap': 'ElasticPool_evt_Swap',
            'source_evt_factory': 'Factory_evt_PoolCreated'
        },
    ]
%}


{{
        kyberswap_compatible_trades(
            blockchain = 'base',
            project = 'kyberswap',
            sources = config_sources
        )
}}