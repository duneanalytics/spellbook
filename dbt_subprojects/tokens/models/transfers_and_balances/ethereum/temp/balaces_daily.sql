{{ config(
        schema = 'temp',
        alias = 'balances_daily',
        materialized = 'table'
        )
}}


select * from {{ref('tokens_ethereum_balances_daily')}}
where block_time > now() - interval '30' day
