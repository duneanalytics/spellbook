{{ config(
        schema = 'tokens_bnb',
        alias = 'balances_daily_agg_base',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_number', 'unique_key'],
        )
}}

with balances_raw as (
{{balances_fix_schema(
    source('tokens_bnb', 'balances_bnb'),
    'bnb',
    '0xb8c77482e45f1f44de1745f52c74426c631bdd52'
)}}
)

{{
    balances_daily_agg(
        balances_raw = 'balances_raw'
    )
}}
