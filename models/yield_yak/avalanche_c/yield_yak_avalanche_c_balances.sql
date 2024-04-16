{{
    config(
        schema = 'yield_yak_avalanche_c',
        alias = 'balances',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['contract_address', 'from_time'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.to_time')]
    )
}}
-- depends_on: {{ ref('yield_yak_avalanche_c_deposits') }}
-- depends_on: {{ ref('yield_yak_avalanche_c_withdraws') }}
-- depends_on: {{ ref('yield_yak_avalanche_c_reinvests') }}
{{
    yield_yak_balances(
        blockchain = 'avalanche_c'
    )
}}
