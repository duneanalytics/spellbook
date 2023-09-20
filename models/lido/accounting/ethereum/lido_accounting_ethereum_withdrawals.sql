{{ config(
        alias = alias('withdrawals'),
        tags = ['dunesql'], 
        partition_by = ['day'],
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["gregshestakovlido", "ppclunghe", "xadcv"]\') }}'
        )
}}

--ref{{'lido_accounting_ethereum_withdrawals'}}

with withdrawals as (
    select block_time          as time
         , block_hash
         , sum(amount) / 1e9   as total_amount
         , sum(CASE
                   WHEN amount / 1e9 BETWEEN 20 AND 32 THEN CAST(amount as double) / 1e9
                   WHEN amount / 1e9 > 32 THEN 32
                   ELSE 0 END) AS withdrawn_principal
    from {{source('ethereum', 'withdrawals')}}
    where address = 0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f
    group by 1,2
)

select time as period
    , block_hash as hash
    , withdrawn_principal*POWER(10, 18) as amount
    , date_trunc('day', time) as day
from withdrawals
where withdrawn_principal != 0