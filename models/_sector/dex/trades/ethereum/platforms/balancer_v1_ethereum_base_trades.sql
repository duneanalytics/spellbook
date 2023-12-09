{{
    config(
        schema = 'balancer_v1_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    balancer_compatible_v1_trades(
        blockchain = 'ethereum',
        project = 'balancer',
        version = '1',
        BPool_evt_LOG_SWAP = 'BPool_evt_LOG_SWAP',
        BPool_call_setSwapFee = 'BPool_call_setSwapFee'
    )
}}
