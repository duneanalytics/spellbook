 {{
  config(
    alias='nft_bridged',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["chuxinh"]\') }}'
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
left join {{ ref('tokens_nft')}} as n 
  on n.blockchain = 'ethereum'
  and n.contract_address = b.`localToken`
{% if is_incremental() %}
  where b.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
group by 1, 2, 3, 4, 5, 6, 7
