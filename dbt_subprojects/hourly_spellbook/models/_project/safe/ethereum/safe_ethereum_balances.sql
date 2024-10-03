{{
   config(
     schema = 'safe_balances',
     alias = 'balances',
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['day', 'address', 'token_address', 'blockchain', 'token_standard', 'token_symbol', 'token_id'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
   )
 }}


with safes as (
    select
        address,
        blockchain
    from {{ ref('safe_ethereum_safes') }}
    where blockchain = 'ethereum'
),
    
balances as (
     {{
       balances_incremental_subset_daily(
             blockchain = 'ethereum',
             address_list  = 'safes',
             start_date = '2021-07-01'
       )
     }}
 )

flagged_balances as (
    select
        b.day,
        b.blockchain,
        b.address,
        b.token_address,
        b.token_standard,
        b.token_id,
        b.token_symbol,
        b.balance,
        b.balance_usd,
        -- Flag balances that changed on the day as "active"
        case when lag(b.balance) over (
            partition by b.address, b.token_address, b.blockchain, b.token_standard, b.token_id
            order by b.day
        ) != b.balance or lag(b.balance) over (
            partition by b.address, b.token_address, b.blockchain, b.token_standard, b.token_id
            order by b.day
        ) is null then 1 else 0 end as is_active
    from balances b
)

select
    day,
    blockchain,
    address,
    token_address,
    token_standard,
    token_id,
    token_symbol,
    sum(balance) as token_balance,
    sum(balance_usd) as balance_usd
from flagged_balances
where token_address not in (
         0xd74f5255d557944cf7dd0e45ff521520002d5748,
         0xe9689028ede16c2fdfe3d11855d28f8e3fc452a3 
  )
group by 1, 2, 3, 4, 5, 6, 7
