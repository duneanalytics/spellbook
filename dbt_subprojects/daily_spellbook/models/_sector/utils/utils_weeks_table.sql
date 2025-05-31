{{ config(
    schema = 'utils',
    alias = 'weeks_table',
    materialized = 'table',
    file_format = 'delta'
    )
}}
-- these are set up as tables instead of incremental models because this guarantees the ordering and compute is negligable.

SELECT timestamp
FROM unnest(
    sequence(
        timestamp '2008-12-29'
        , cast(date_trunc('week', now() + interval '3' day) as timestamp)  -- add some padding to account for materialization lag
        , interval '7' day
        )
    ) as foo(timestamp)
order by timestamp asc