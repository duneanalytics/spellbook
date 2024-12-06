{{ config(
        schema = 'temp',
        alias = 'balances_raw',
        materialized = 'table'
        )
}}


select * from {{source('tokens_ethereum','balances_daily')}}
where block_time > now() - interval '30' day
