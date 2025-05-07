{{ config(
        schema = 'tokens_base',
        alias = 'balances_daily_agg_base',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['day', 'unique_key'],
        partition_by = ['day'],
        )
}}

with balances_raw as (
{{balances_fix_schema(source('tokens_base', 'balances_base'), 'base')}}
)

{{
    balances_daily_agg(
        balances_raw = 'balances_raw'
    )
}}
