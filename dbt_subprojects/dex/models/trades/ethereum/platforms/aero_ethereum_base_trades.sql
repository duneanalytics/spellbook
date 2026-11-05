{{
    config(
        schema = 'aero_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{ aero_trades(blockchain = 'ethereum', cl_factory = '0xf4fd2e6c5b60c5b365cd1b96dd6245e6d60414c2') }}
