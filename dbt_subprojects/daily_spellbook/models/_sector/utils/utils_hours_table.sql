{{ config(
    schema = 'utils',
    alias = 'hours_table',
    materialized = 'incremental',
    file_format = 'delta',
    unique_key = 'timestamp',
    incremental_strategy = 'merge',
}}


with days as (
    select * from {{ref('utils_days_table')}}
    {%if is_incremental() %}
    where {{incremental_predicate('timestamp')}}
    {%endif%}
)

SELECT * FROM (
        SELECT date_add('hour', hour, day) as timestamp
        FROM (
            SELECT timestamp as day 
            FROM days
        )
        CROSS JOIN unnest(sequence(0, 23)) AS h(hour)
)