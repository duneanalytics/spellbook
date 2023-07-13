{{config(
        tags = ['dunesql', 'static'],
        schema = 'test_timestamp',
        alias = alias('table4'),
        materialized = 'table',
        file_format = 'delta'
)}}

select
    NOW() as test_now
