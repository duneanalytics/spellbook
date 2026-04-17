{{ config(
    schema = 'dwh_421_test',
    alias = 'test_table',
    materialized = 'table',
    file_format = 'delta',
    tags = ['dwh_421_test']
) }}

SELECT 1 AS n, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6) WITH TIME ZONE) AS created_at
