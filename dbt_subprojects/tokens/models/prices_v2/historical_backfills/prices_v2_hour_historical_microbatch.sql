{{ config(
        schema='prices_v2',
        alias = 'hour_historical_microbatch',
        materialized = 'incremental',
        file_format = 'delta',
        partition_by = ['date'],
        incremental_strategy = 'microbatch',
        begin='2024-09-01',
        batch_size='month',
        event_time =' timestamp',
        unique_key = ['blockchain', 'contract_address', 'timestamp']
        )
}}

-- this is the max date of the historical batched backfill, after which the incremental model takes over
-- we currently need to setup to have both monthly historical batches, but smaller incremental batches with a daily lookback.
-- currently all data in historical is rematerialized in the incremental model to prevent any chance of duplicates.
-- after this end_date, this model will still run but is setup to produce no outputs.
-- you should set this date to maximum the start of the current batch, to make sure the last batch is always running empty once in prod.
{% set end_date = '2025-01-01' %}   -- assume exclusive

-- depends_on: {{ ref('prices_v2_day_sparse') }} (this is needed because it's only used in the conditional block)

WITH sparse_prices as (
    select
        *
        , lead(timestamp) over (partition by blockchain, contract_address order by timestamp asc) as next_update
    from (
        select
            blockchain
            , contract_address
            , timestamp
            , price
            , volume
            , source
            , date
            , source_timestamp
        from {{ ref('prices_v2_hour_sparse') }}
        where 1=1
            and timestamp > now() - interval '90' day   -- todo: remove temp filter
            and timestamp < timestamp '{{end_date}}' -- don't process any data from end_date on
        UNION ALL
        SELECT * FROM (
            select
                blockchain
                , contract_address
                , max(timestamp)        -- we get the last updated price
                , max_by(price,timestamp) as price
                , max_by(volume,timestamp) as volume
                , max_by(source,timestamp) as source
                , max(date) as date
                , max_by(source_timestamp,timestamp) as source_timestamp
            -- we need render() here to not autofilter the model based on the batch timestamps
            from {{ ref('prices_v2_day_sparse').render() }}  -- because incremental windows always start at start of day, we cheat a little and use day here
            where timestamp < (select min(timestamp) from {{ref('utils_days')}})
            group by blockchain, contract_address
        )
    )
)

-- construct the time spline we want to fill
-- we have to do days first because the sequence can have max 10000 entries
, timeseries as (
    select * from (
       select date_add('hour',hour, day) as timestamp
       from (select timestamp as day from {{ref('utils_days')}}) --this is auto-filtered for the microbatch
       cross join unnest(sequence(0, 23)) as h(hour)
   )
   where timestamp <= now() --don't overshoot
)

SELECT
    p.blockchain
    , p.contract_address
    , t.timestamp
    , p.price
    , p.volume
    , p.source
    , date_trunc('day', t.timestamp) as date
    , p.source_timestamp
FROM timeseries t
INNER JOIN sparse_prices p
    on p.timestamp <= t.timestamp
    and (p.next_update is null or p.next_update > t.timestamp)
