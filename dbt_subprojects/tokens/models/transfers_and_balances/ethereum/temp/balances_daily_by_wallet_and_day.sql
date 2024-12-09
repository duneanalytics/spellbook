{{ config(
        schema = 'temp',
        alias = 'balances_daily_by_wallet_and_day',
        materialized = 'table',
        partition_by = ['day','address_partition']
        )
}}


select
*
,cast(varbinary_substring(address, 1,1) as varchar) as address_partition
from {{ref('tokens_ethereum_balances_daily_agg')}}
where day > now() - interval '1000' day and token_standard in ('native', 'erc20')
