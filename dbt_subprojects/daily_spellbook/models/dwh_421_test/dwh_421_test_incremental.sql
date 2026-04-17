{{ config(
    schema = 'dwh_421_test',
    alias = 'test_incremental',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['n'],
    tags = ['dwh_421_test']
) }}

SELECT
    1 AS n,
    'hello' AS label
