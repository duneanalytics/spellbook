{{ config(
        schema = 'dbt_cloud_test'

        ,alias = 'error_model'
        ,materialized = 'table'
        )
}}
select 'this_model_fails_with_error' as comment
. 10/0 as error
, now() as last_executed
from {{ ref('dbt_cloud_test_good_model') }}
