{{ config(
        schema = 'dbt_cloud_test'
        ,tags = ['prod_exclude']
        ,alias = 'good_model'
        ,materialized = 'table'
        )
}}
select 'this_model_works' as comment
, now() as last_executed
