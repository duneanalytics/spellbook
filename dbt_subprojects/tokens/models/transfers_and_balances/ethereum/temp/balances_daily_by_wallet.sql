{{ config(
        schema = 'temp',
        alias = 'balances_daily_by_wallet',
        materialized = 'table',
        partition_by = ['address_partition']
        )
}}


select
*
,cast(varbinary_substring(address, 1,1) as varchar) as address_partition
from {{ref('tokens_ethereum_balances_daily_agg')}}
where day > now() - interval '3000' day and token_standard in ('native', 'erc20')
