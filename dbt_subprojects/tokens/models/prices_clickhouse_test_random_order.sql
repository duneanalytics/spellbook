{{ config(
        schema='prices_test',
        alias = 'clickhouse_copy_random_order',
        materialized='table',
        file_format = 'delta',
        partition_by = ['year','month','blockchain'],
        tags = ['static']
        )
}}

select * 
from {{ source("dune", "prices_day_raw", database="dune") }}
order by random()