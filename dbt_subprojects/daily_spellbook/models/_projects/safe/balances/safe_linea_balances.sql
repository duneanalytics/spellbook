{{
   config(
     schema = 'safe_linea',
     alias = 'balances',
     partition_by = ['day'],
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
    from {{ source('safe_linea','safes') }}
    where blockchain = 'linea'
),

balances as (
     {{
       balances_incremental_subset_daily(
             blockchain = 'linea',
             address_list  = 'safes',
             start_date = '2021-07-01'
       )
     }}
)

select * from balances
where token_standard in ('native', 'erc20')
and token_address not in (
            0xd74f5255d557944cf7dd0e45ff521520002d5748, --$9.8B were minted in a hack in 2023, all of which are stored in a Safe. Filtering out.
            0xe9689028ede16c2fdfe3d11855d28f8e3fc452a3 )
