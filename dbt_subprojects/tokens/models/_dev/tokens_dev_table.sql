{{ config(
    schema = 'dev',
    materialized = 'table',
    file_format = 'delta',
) }}

--stamp 2
select 3 as stamp
