{{
   config(
     schema = 'safe_balances',
     alias = 'balances',
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['day', 'address', 'token_address', 'blockchain'],
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

select
    b.day,
    b.blockchain,
    b.address,
    b.token_address,
    b.token_standard,
    b.token_id,
    b.token_symbol,
    sum(b.balance) as token_balance,
    sum(balance_usd) as balance_usd
from balances b
where b.token_address not in (
             0xd74f5255d557944cf7dd0e45ff521520002d5748, --$9.8B were minted in a hack in 2023, all of which are stored in a Safe. Filtering out.
             0xe9689028ede16c2fdfe3d11855d28f8e3fc452a3 ) -- BUBBLE
group by 1, 2, 3, 4, 5, 6, 7
