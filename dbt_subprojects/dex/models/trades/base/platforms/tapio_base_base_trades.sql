{{
    config(
        schema = 'tapio_base',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    tapio_compatible_trades(
        blockchain = 'base',
        project = 'tapio',
        version = '1',
        factory_create_pool_function = 'selfpeggingassetfactory_call_createpool',
        factory_create_pool_evt = 'selfpeggingassetfactory_evt_poolcreated',
        spa_token_swapped_evt = 'selfpeggingasset_evt_tokenswapped'
    )
}}
