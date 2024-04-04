{{
    config(
        schema = 'yield_yak_avalanche_c',
        alias = 'balances',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['contract_address', 'from_time'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{
    yield_yak_balances(
        blockchain = 'avalanche_c'
    )
}}
