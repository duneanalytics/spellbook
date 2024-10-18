{{
    config(
        schema = 'safe_optimism',
        alias = 'balances',
        partition_by = ['day'],
        materialized = 'incremental',
        incremental_strategy = 'merge',
        file_format = 'delta',
        unique_key = ['day', 'blockchain', 'address', 'token_address'],
        post_hook = '{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "safe",
                                    \'["safeintern"]\') }}'
    )
}}

with safes as (
    -- Capture all safes from a reference table
    select
        address,
        blockchain
    from {{ ref('safe_optimism_safes') }}
    where blockchain = 'optimism'
),
balances as (
     {{
       balances_incremental_subset_daily(
             blockchain = 'optimism',
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
