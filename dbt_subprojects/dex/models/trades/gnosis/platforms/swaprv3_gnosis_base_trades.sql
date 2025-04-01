{{ config(
    schema = 'swaprv3_gnosis',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{{
    swaprv3_compatible_trades(
        blockchain = 'gnosis',
        project = 'swaprv3',
        version = '3',
        Pair_evt_Swap = source('swaprv3_gnosis', 'AlgebraPool_evt_Swap'),
        Factory_evt_PoolCreated = source('swaprv3_gnosis', 'SwaprV3Factory_evt_Pool'),
        Fee_evt = source('swaprv3_gnosis', 'AlgebraPool_evt_Fee'),
        taker_column_name = 'recipient'
    )
}}