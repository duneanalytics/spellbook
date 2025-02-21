{{ config(
    schema = 'utils',
    alias = 'days',
    materialized = 'view'
}}


select * from {{ref('utils_days_table')}}
where timestamp <= now()