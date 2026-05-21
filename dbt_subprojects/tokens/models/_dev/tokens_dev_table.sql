{{ config(
    schema = 'dev',
    materialized = 'table',
    file_format = 'delta',
    tags = ['prod_exclude']
) }}

--stamp 2
select 2 as stamp
