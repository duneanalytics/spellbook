{{ config(
        schema='prices_test',
        alias = 'sqlmesh_copy_ordered',
        materialized='table',
        file_format = 'delta',
        partition_by = ['year','month','blockchain'],
        tags = ['static']
        )
}}

select * 
from "hive"."sqlmesh__prices_v2"."prices_v2__day_clickhouse_replica__2289764117__dev"
order by blockchain, contract_address, timestamp