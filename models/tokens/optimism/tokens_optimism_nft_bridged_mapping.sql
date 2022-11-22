 {{
  config(
    alias='nft_bridged_mapping',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["chuxin"]\') }}'
  )
}}
select 
  n.category as category
  ,b.`remoteToken` as contract_address
  ,n.name
  ,n.standard
  ,n.symbol
  ,b.`localToken` as contract_address_l1
from {{ source('optimism_ethereum','L1ERC721Bridge_evt_ERC721BridgeInitiated') }} as b 
left join {{ ref('tokens_ethereum_nft')}} as n 
  on n.contract_address = b.`localToken`
group by 1, 2, 3, 4, 5, 6
