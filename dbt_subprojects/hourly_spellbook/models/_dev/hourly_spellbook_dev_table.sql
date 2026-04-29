{{ config(
    schema = 'dev',
    materialized = 'table',
    file_format = 'delta'
) }}

--stamp 1
select 1 as stamp
