
{{ config(
    schema = 'metropolis_sonic',
    alias  = 'base_trades',
    materialized = 'incremental',
    file_format  = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}


   {{
    metropolis_compatible_trades(
        blockchain = 'sonic',
        project    = 'metropolis',
        version    = '1',
        factory_create_pool_evt = 'v2factory_evt_paircreated',
        token_swapped_function = 'all_swaps'
    )
}}

   
