{{ config(
        schema = 'temp',
        alias = 'balances_daily_by_wallet',
        materialized = 'table',
        partition_by = ['address_partition']
        )
}}


select
*
, day as block_time
,cast(varbinary_substring(address, 0,2) as varchar) as address_partition
from {{ref('tokens_ethereum_balances_daily_agg')}}
where day > now() - interval '30' day
