{{ config(
        schema='test_test',
        alias = 'dummy',
        materialized='table',
        file_format = 'delta',
        tags = ['static']
        )
}}

select 1 as dummy   