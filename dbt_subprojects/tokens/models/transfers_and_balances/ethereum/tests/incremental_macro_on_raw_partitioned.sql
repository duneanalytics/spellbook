{{
   config(
     schema = 'tests',
     alias = 'macro_on_raw_partitioned',
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
    from {{ ref('safe_ethereum_safes') }}
    where blockchain = 'ethereum'
    limit 100000
),

balances as (
     {{
       balance_macro_copy(
             blockchain = 'ethereum',
             address_list  = 'safes',
             start_date = '2021-07-01',
             balances_source = ref('balances_raw_by_wallet')
       )
     }}
)

select * from balances
