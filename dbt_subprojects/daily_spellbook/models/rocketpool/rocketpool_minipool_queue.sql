{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_queue',
    materialized = 'table'
    )
}}

with
minipool as (
    select minipool
    from
       {{ ref ('rocketpool_minipool_created_destroyed') }}
)
,
minipool_enqueued as (
    select
        evt_block_time as t,
        minipool
    from
        {{ source('rocketpool_ethereum','RocketMinipoolQueue_evt_MinipoolEnqueued') }}
)
,
minipool_dequeued as (
    select
        evt_block_time as t,
        minipool
    from
        {{ source('rocketpool_ethereum','RocketMinipoolQueue_evt_MinipoolDequeued') }}
)
,
minipool_removed as (
    select
        evt_block_time as t,
        minipool
    from
        {{ source('rocketpool_ethereum','RocketMinipoolQueue_evt_MinipoolRemoved') }}
)

select
    minipool.minipool,
    enq.t as enqueued_time,
    coalesce(deq.t, rem.t) as dequeued_time,
    date_diff('day', enq.t, coalesce(deq.t, rem.t)) as queue_days,
    date_diff('hour', enq.t, coalesce(deq.t, rem.t)) as queue_hrs
from
    minipool
left join minipool_enqueued as enq on minipool.minipool = enq.minipool
left join minipool_dequeued as deq on minipool.minipool = deq.minipool
left join minipool_removed as rem on minipool.minipool = rem.minipool
