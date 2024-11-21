{{ config(
        tags = ['monitoring'],
        schema='prices',
        alias = 'usd_monitor',
        materialized = 'incremental',
        incremental_strategy = 'append'
        )
}}

-- monitor to gather quantile information about prices latency
select
    now() as recorded_at
    ,cast(qdigest_agg(latency_seconds) as varbinary) as latency_seconds_digest
from (
    select
    blockchain, contract_address, date_diff('second',max(minute),now()) as latency_seconds
    from {{source('prices','usd')}}
    where minute >= now() - interval '7' day    -- we'll consider anything that's more then 7 days late as stale tokens
    group by 1,2
)
group by 1
