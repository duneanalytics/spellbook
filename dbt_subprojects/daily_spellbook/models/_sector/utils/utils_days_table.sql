{{ config(
    schema = 'utils',
    alias = 'days_table',
    materialized = 'table',
    file_format = 'delta'
    )
}}
-- these are set up as tables instead of incremental models because this guarantees the ordering and compute is negligable.

SELECT timestamp
FROM unnest(
    sequence(
        timestamp '2009-01-03'
        , cast(date_trunc('day', now()) as timestamp)+ interval '3' day  -- add some padding to account for materialization lag
        , interval '1' day
        )
    ) as foo(timestamp)
order by timestamp asc