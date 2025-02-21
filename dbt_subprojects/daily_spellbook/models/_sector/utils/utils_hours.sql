{{ config(
    schema = 'utils',
    alias = 'hours',
    materialized = 'view'
    )
}}


select * from {{ref('utils_hours_table')}}
where timestamp <= now()