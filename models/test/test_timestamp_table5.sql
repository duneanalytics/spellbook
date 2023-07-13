{{config(
        tags = ['dunesql', 'static'],
        schema = 'test_timestamp',
        alias = alias('table5'),
        materialized = 'table',
        file_format = 'delta'
)}}

select
    block_time as test_block_time
from {{ source('ethereum', 'transactions') }}
limit 1
