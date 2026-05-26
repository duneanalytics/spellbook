{{ config(
    schema = 'dev',
    materialized = 'view',
    tags = ['prod_exclude']
) }}

--stamp 2
select 2 as stamp
