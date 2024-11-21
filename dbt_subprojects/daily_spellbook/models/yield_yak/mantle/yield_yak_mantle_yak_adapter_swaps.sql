{{
    config(
        schema = 'yield_yak_mantle',
        alias = 'yak_adapter_swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_index', 'adapter_evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    yield_yak_yak_adapter_swaps(
        blockchain = 'mantle'
    )
}}
