{{ config(
        schema = 'temp',
        alias = 'balances_raw_by_wallet',
        materialized = 'table',
        partition_by = ['address_partition']
        )
}}


select
*
,cast(varbinary_substring(address, 0,2) as varchar) as address_partition
from {{ref('tokens_ethereum_balances')}}
where block_time > now() - interval '30' day
