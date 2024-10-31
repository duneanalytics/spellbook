{{ config(
        schema = 'dbt_cloud_test'

        ,alias = 'good_model'
        ,materialized = 'table'
        )
}}
select 'this_model_works' as comment
, now() as last_executed
