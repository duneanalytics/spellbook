{{ config(
    schema = 'utils',
    alias = 'months_table',
    materialized = 'table',
    file_format = 'delta'
    )
}}
-- these are set up as tables instead of incremental models because this guarantees the ordering and compute is negligable.

SELECT timestamp
FROM unnest(
    sequence(
        timestamp '2009-01-01'
        , cast(date_trunc('month', now() + interval '3' day) as timestamp)  -- add some padding to account for materialization lag
        , interval '1' month
        )
    ) as foo(timestamp)
order by timestamp asc