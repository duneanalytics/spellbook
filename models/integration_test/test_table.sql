{{ config(materialized='table', alias='test_table2') }}

select {{ env_var('S3_BUCKET', 'local') }}