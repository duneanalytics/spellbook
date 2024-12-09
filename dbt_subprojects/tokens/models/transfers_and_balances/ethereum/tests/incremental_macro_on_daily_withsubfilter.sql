{{
   config(
     schema = 'tests',
     alias = 'macro_on_daily_withsubfilter',
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['day', 'address', 'token_address'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
   )
 }}

with safes as (
    select
        address,
        blockchain
    from safe_ethereum.safes
    where blockchain = 'ethereum'
),

balances as (
     {{
       balance_macro_copy_withsubfilter(
             blockchain = 'ethereum',
             address_list  = 'safes',
             start_date = '2021-07-01',
             balances_source = ref('balances_daily')
       )
     }}
)

select * from balances
