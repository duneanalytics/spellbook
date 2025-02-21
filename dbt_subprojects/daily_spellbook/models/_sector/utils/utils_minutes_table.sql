{{ config(
    schema = 'utils',
    alias = 'hours_table',
    materialized = 'incremental',
    file_format = 'delta',
    unique_key = 'timestamp',
    incremental_strategy = 'merge',
}}


with hours as (
    select * from {{ref('utils_hours_table')}}
    {%if is_incremental() %}
    where {{incremental_predicate('timestamp')}}
    {%endif%}
)

SELECT * FROM (
        SELECT date_add('minute', minute, hour) as timestamp
        FROM (
            SELECT timestamp as hour 
            FROM hours
        )
        CROSS JOIN unnest(sequence(0, 59)) AS m(minute)
)