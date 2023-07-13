{{config(
        tags = ['dunesql', 'static'],
        schema = 'test_timestamp',
        alias = alias('table2'),
        materialized = 'table',
        file_format = 'delta'
)}}

select
    TIMESTAMP '2022-1-8 10:1:17' as test_timestamp,
    DATE '2022-1-8' as test_date
