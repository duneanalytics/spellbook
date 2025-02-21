{{ config(
    schema = 'utils',
    alias = 'minutes_table',
    materialized = 'table',
    file_format = 'delta'
    )
}}


with hours as (
    select * from {{ref('utils_hours_table')}}
)

SELECT * FROM (
        SELECT date_add('minute', minute, hour) as timestamp
        FROM (
            SELECT timestamp as hour 
            FROM hours
        )
        CROSS JOIN unnest(sequence(0, 59)) AS m(minute)
)
order by timestamp asc