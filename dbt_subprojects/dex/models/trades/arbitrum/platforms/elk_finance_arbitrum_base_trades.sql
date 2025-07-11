{{ config(
    schema = 'elk_finance_arbitrum',
    alias = 'base_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
) }}

{{ elk_compatible_v1_trades(
    blockchain = 'arbitrum',
    project = 'elk_finance',
    version = '1',
    Pair_evt_Swap = source('elk_finance_arbitrum', 'ElkPair_evt_Swap'),
    Factory_evt_PairCreated = source('elk_finance_arbitrum', 'ElkFactory_evt_PairCreated'),
    pair_column_name = 'pair'
) }}
