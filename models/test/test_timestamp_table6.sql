{{config(
        tags = ['dunesql', 'static'],
        schema = 'test_timestamp',
        alias = alias('table6'),
        materialized = 'table',
        file_format = 'delta'
)}}

select
    cast(block_time as timestamp) as test_block_time
from {{ source('ethereum', 'transactions') }}
limit 1
