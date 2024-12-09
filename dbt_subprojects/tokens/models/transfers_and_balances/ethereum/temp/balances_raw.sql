{{ config(
        schema = 'temp',
        alias = 'balances_raw',
        materialized = 'table'
        )
}}


select * from {{ref('tokens_ethereum_balances')}}
where block_time > now() - interval '30' day
