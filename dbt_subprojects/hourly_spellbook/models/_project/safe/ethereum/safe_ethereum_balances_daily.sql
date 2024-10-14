
{{
   config(
     schema = 'safe_ethereum_balances',
     alias = 'daily',
     materialized = 'view'
   )
}}

select
  *,
  coalesce(token_id, uint256 '1') as unique_key_id 
from {{ ref('safe_ethereum_balances') }}
where token_address not in (
            0xd74f5255d557944cf7dd0e45ff521520002d5748, --$9.8B were minted in a hack in 2023, all of which are stored in a Safe. Filtering out.
            0xe9689028ede16c2fdfe3d11855d28f8e3fc452a3 )
