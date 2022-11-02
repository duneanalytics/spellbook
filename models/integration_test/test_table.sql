{{ config(materialized='table', alias='test_table2') }}

select {{ env_var('DBT_ENV_CUSTOM_ENV_S3_BUCKET', 'local') }}