{{ config(
        schema = 'test_dont_merge',
        alias = alias('partitioned_by_time'),
        tags = ['dunesql'],
        partition_by = ['partition_column']
        )
}}

select
    date_trunc('day', block_time) as partition_column
from {{ source('ethereum','transactions') }}
limit 20
