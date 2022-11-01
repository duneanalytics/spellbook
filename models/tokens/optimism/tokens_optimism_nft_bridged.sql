 {{
  config(
    alias='nft_bridged',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['contract_address'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["chuxinh"]\') }}'
  )
}}
select 
  'optimism' as blockchain
  ,n.category as category
  ,b.`remoteToken` as contract_address
  ,n.name
  ,n.standard
  ,n.symbol as symbol
  ,b.`localToken` as contract_address_l1
from {{ source('optimism_ethereum','L1ERC721Bridge_evt_ERC721BridgeInitiated') }} as b 
left join {{ ref('tokens_nft')}} as n 
  on n.blockchain = 'ethereum'
  and n.contract_address = b.`localToken`
{% if is_incremental() %}
  where b.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
group by 1, 2, 3, 4, 5, 6, 7
