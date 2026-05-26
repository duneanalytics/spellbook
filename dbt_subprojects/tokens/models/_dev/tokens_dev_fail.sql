{{ config(
    schema = 'dev',
    materialized = 'view',
    tags = ['prod_exclude']
) }}

select *
from nonexistent_catalog.nonexistent_schema.nonexistent_table_for_failure_test
