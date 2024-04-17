{{ config(
        schema = 'tokens_ethereum',
        alias = 'balances_latest',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xRob"]\') }}'
        )
}}

with balances_latest as (
select * from {{ref('tokens_ethereum_balances_daily_agg')}}
where next_update_day is null -- last known record
)

{{
    balances_enrich(
        balances_raw = 'balances_latest',
    )
}}

