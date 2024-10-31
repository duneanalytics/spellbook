{{ config(
        schema = 'dbt_cloud_test'

        ,alias = 'skipped_model'
        ,materialized = 'table'
        )
}}
select 'this_model_skips' as comment
, now() as last_executed
from {{ ref('dbt_cloud_test_error_model') }}
