{{config(
        tags = ['dunesql'],
        schema = 'test_timestamp',
        alias = alias('view'),
        materialized = 'view',
        file_format = 'delta'
)}}

select
    TIMESTAMP '2022-1-8' as test_timestamp,
    DATE '2022-1-8' as test_date
