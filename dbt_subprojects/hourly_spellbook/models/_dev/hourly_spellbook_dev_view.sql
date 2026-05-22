{{ config(
    schema = 'dev',
    materialized = 'view'
) }}

--stamp 2
select 2 as stamp
