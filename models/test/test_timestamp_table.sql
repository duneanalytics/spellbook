{{config(
        tags = ['dunesql', 'static'],
        schema = 'test_timestamp',
        alias = alias('table'),
        materialized = 'table',
        file_format = 'delta'
)}}

select
    TIMESTAMP '2022-1-8' as test_timestamp
