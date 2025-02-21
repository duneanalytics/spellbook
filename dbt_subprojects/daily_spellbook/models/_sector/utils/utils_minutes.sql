{{ config(
    schema = 'utils',
    alias = 'minutes',
    materialized = 'view'
}}


select * from {{ref('utils_minutes_table')}}
where timestamp <= now()