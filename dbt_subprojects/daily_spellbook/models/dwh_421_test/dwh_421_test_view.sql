{{ config(
    schema = 'dwh_421_test',
    alias = 'test_view',
    materialized = 'view',
    tags = ['dwh_421_test']
) }}

SELECT * FROM {{ ref('dwh_421_test_table') }}
