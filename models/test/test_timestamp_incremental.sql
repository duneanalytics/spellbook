{{config(
        tags = ['dunesql'],
        schema = 'test_timestamp',
        alias = alias('incremental'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['test_timestamp']
)}}

select
    TIMESTAMP '2022-1-8' as test_timestamp,
    DATE '2022-1-8' as test_date
