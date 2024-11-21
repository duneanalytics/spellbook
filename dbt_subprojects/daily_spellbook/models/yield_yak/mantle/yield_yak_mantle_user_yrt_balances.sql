{{
    config(
        schema = 'yield_yak_mantle',
        alias = 'user_yrt_balances',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['user_address', 'contract_address', 'from_time'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.to_time')]
    )
}}

{{
    yield_yak_user_yrt_balances(
        blockchain = 'mantle'
    )
}}
