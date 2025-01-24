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
-- depends_on: {{ ref('prices_v2_hour_historical_microbatch') }}

-- this should be the same as the end_date in prices_v2_hour_historical_microbatch (there's more comments there on the setup)
{% set start_date = '2024-12-01' %}

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
            and timestamp >= timestamp '{{start_date}}'
            {% if is_incremental() %}
            and {{ incremental_predicate('timestamp') }}
            {% endif %}
        -- We also need to add the last known prices from before the current window, to forward fill them
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
            -- this can probably be more optimized to use the filled day table and query for 1 specific date.
            from {{ ref('prices_v2_day_sparse') }}  -- because incremental windows always start at start of day, we can use day here
            where 1=1
                {% if is_incremental() %}
                and not {{ incremental_predicate('timestamp') }} -- not in the current incremental window (so before that)
                {% else %}
                and not timestamp >= timestamp '{{start_date}}'  -- on full-refresh, take the data from before the start_date
                {% endif %}
            group by blockchain, contract_address
        )
    )
)

-- construct the time spline we want to fill
-- we have to do days first because the sequence can have max 10000 entries
, timeseries as (
    select * from (
       select date_add('hour',hour, day) as timestamp
       from (
            select timestamp as day from {{ref('utils_days')}}
            where 1=1
                and timestamp >= (select min(greatest(timestamp, timestamp '{{start_date}}')) from sparse_prices)
                and timestamp <= now()
       )
       cross join unnest(sequence(0, 23)) as h(hour)
   )
   where 1=1
       and timestamp <= now()   -- safety to not overshoot the hours
       and timestamp >= timestamp '{{start_date}}'  -- not needed, but safety to never have any duplicates
       {% if is_incremental() %}
       and {{ incremental_predicate('timestamp') }}
       {% endif %}
)

, incremental_forward_fill as (
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
)

select
    *
from incremental_forward_fill
-- on the first run (full-refresh), copy the historical microbatches here
{% if not is_incremental() %}
union all
select
    *
from {{ ref('prices_v2_hour_historical_microbatch') }}
{% endif %}



