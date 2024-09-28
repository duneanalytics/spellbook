{{ config(
        schema = 'tokens_avalanche_c',
        alias = 'balances_daily_agg_base',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_number', 'unique_key'],
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_avalanche_c', 'balances_avalanche_c'), 'avalanche_c', '0x0000000000000000000000000000000000000000')}}
)

{{
    balances_daily_agg(
        balances_raw = 'balances_raw'
    )
}}
