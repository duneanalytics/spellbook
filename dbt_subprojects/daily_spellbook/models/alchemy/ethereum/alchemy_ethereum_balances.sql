{{
  config(
    schema = 'alchemy_ethereum',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_address', 'unique_key_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

with
alchemy_addresses as (
  select
    address
  from {{ref('alchemy_smart_account_addresses')}}
)

,balances as (
    {{
      balances_incremental_subset_daily(
            blockchain = 'ethereum',
            address_list  = 'alchemy_addresses',
            start_date = '2023-04-13'
      )
    }}
)

select
  *,
  coalesce(token_id, uint256 '1') as unique_key_id ---using this to add a unique key to the table as erc20 has no token_id
from balances
where token_address not in (
            0xd74f5255d557944cf7dd0e45ff521520002d5748, --$9.8B were minted in a hack in 2023, all of which are stored in a Safe. Filtering out.
            0xe9689028ede16c2fdfe3d11855d28f8e3fc452a3 )