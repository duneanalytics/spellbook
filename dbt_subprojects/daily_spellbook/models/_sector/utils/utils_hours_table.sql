{{ config(
    schema = 'utils',
    alias = 'hours_table',
    materialized = 'table',
    file_format = 'delta'
    )
}}


with days as (
    select * from {{ref('utils_days_table')}}
)

SELECT * FROM (
        SELECT date_add('hour', hour, day) as timestamp
        FROM (
            SELECT timestamp as day 
            FROM days
        )
        CROSS JOIN unnest(sequence(0, 23)) AS h(hour)
)
order by timestamp asc