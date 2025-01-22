{{
    config(
        schema = 'yield_yak_avalanche_c',
        alias = 'yak_swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_number', 'tx_index', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    yield_yak_yak_swaps(
        blockchain = 'avalanche_c'
    )
}}
