{{ config(
    schema = 'utils',
    alias = 'quarter_table',
    materialized = 'table',
    file_format = 'delta'
    )
}}
-- these are set up as tables instead of incremental models because this guarantees the ordering and compute is negligable.

SELECT timestamp
FROM unnest(
    sequence(
          timestamp '2009-01-01'
        , cast(
            date_trunc('quarter', now() + interval '3' day)
            as timestamp
          )
        , interval '3' month
        )
    ) AS foo(timestamp)
ORDER BY timestamp ASC