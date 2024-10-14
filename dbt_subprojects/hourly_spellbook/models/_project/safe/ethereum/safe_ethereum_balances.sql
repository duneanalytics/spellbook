{{
   config(
     schema = 'safe_ethereum',
     alias = 'balances',
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     unique_key = ['day', 'address', 'token_address', 'unique_key_id'],
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

select * from balances
