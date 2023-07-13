{{config(
        tags = ['dunesql', 'static'],
        schema = 'test_timestamp',
        alias = alias('table8'),
        materialized = 'table',
        file_format = 'delta'
)}}

select
    TIMESTAMP '2022-1-8 10:10:17.123 UTC' as test_timestamp
