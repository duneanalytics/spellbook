{{ config(
        schema='prices_v2',
        alias = 'hour',
        materialized = 'incremental',
        file_format = 'delta',
        partition_by = ['date'],
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address', 'timestamp'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.timestamp')]
        )
}}

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
            and timestamp > now() - interval '90' day
            {% if is_incremental() %}
            and {{ incremental_predicate('timestamp') }}
            {% endif %}
        -- If we're running incremental, we also need to add the last known prices from before the incremental window, to forward fill them
        {% if is_incremental() %}
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
            from {{ ref('prices_v2_day_sparse') }}  -- because incremental windows always start at start of day, we cheat a little and use day here
            where not {{ incremental_predicate('timestamp') }} -- not in the current incremental window (so before that)
            group by blockchain, contract_address
        )
        {% endif %}
    )
)

-- construct the time spline we want to fill
-- we have to do days first because the sequence can have max 10000 entries
, timeseries as (
    select * from (
       select date_add('hour',hour, day) as timestamp
       from unnest(
             sequence(cast((select date_trunc('day', min(timestamp)) from sparse_prices) as timestamp)
                    , cast(date_trunc('day', now()) as timestamp)
                    , interval '1' day
                    )
             ) as foo(day)
       cross join unnest(sequence(0, 23)) as h(hour)
   )
   where 1=1
       and timestamp <= now()
       {% if is_incremental() %}
       and {{ incremental_predicate('timestamp') }}
       {% endif %}
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
