{{ config(
        schema = 'tokens_bitcoin',
        alias = 'balances_daily_agg_base',
        file_format = 'delta',
        materialized='incremental',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['day', 'unique_key'],
        partition_by = ['day']
        )
}}

with balances_raw as (
    select
        'bitcoin' as blockchain,
        block_time,
        block_number,
        wallet_address as address,
        null as token_address,
        'native' as token_standard,
        null as token_id,
        amount_raw as balance_raw
    from {{ source('bitcoin', 'transfers') }}
    where amount_raw > 0
)

{{
    balances_daily_agg(
        balances_raw = 'balances_raw'
    )
}} 