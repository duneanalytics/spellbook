 {{
  config(
    alias = 'nft_bridged_mapping',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["chuxin"]\') }}'
  )
}}
select distinct
   b.localToken as contract_address
  ,n.name
  ,n.standard
  ,n.symbol
  ,b.remoteToken as contract_address_l1
from {{ source('ovm_optimism','L2ERC721Bridge_evt_ERC721BridgeFinalized') }} as b
left join {{ ref('tokens_ethereum_nft')}} as n
  on n.contract_address = b.localToken
