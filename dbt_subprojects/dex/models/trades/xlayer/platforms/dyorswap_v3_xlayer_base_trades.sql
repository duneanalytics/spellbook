{{
    config(
        schema = 'dyorswap_v3_xlayer',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        pre_hook = "{{ create_schema(this) }}"
    )
}}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'xlayer',
        project = 'dyorswap',
        version = '3',
        Pair_evt_Swap = source('dyorswap_v3_xlayer', 'DYORV3Pool_evt_Swap'),
        Factory_evt_PoolCreated = source('dyorswap_v3_xlayer', 'DYORV3Factory_evt_PoolCreated')
    )
}}
