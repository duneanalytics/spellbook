{{
    config(
        schema = 'aero_robinhood',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{ aero_trades(blockchain = 'robinhood', cl_factory = '0xc63d7338c178e805c544b8f897066bd3bbcbe136') }}
