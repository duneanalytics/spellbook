{{config(
        tags = ['dunesql', 'static'],
        schema = 'test_timestamp',
        alias = alias('table3'),
        materialized = 'table',
        file_format = 'delta'
)}}

select
    DATE '2022-1-8' as test_date
