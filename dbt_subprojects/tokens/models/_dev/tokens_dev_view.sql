{{ config(
    schema = 'dev',
    materialized = 'view'
) }}

--stamp 1
select 1 as stamp
