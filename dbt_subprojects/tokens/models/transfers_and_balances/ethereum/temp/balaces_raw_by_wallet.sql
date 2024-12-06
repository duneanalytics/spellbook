{{ config(
        schema = 'temp',
        alias = 'balances_raw_by_wallet',
        materialized = 'table',
        partition_by = ['address_partition']
        )
}}


select
*
,varbinary_substring(address, 0,2) as address_partition
from {{ref('tokens_ethereum_balances_daily')}}
where block_time > now() - interval '30' day
