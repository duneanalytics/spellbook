 {{
  config(
    tags = ['dunesql'],
    alias= alias('tokens_optimism_nft_generated'),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["msilb7"]\') }}'
  )
}}
select
   b.edition_address as contract_address
  ,b.edition_name AS name
  ,b.edition_symbol AS symbol
  ,b.royalty_pct
  ,'Sound.xyz' as platform
from {{ ref('sound_xyz_optimism_edition_metadata') }} as b
GROUP BY 1,2,3,4

UNION ALL

select
   b.edition_address as contract_address
  ,b.edition_name AS name
  ,b.edition_symbol AS symbol
  ,b.royalty_pct
  ,'Zora' as platform
from {{ ref('zora_optimism_edition_metadata') }} as b
GROUP BY 1,2,3,4

UNION ALL

select
   b.nft_contract_address as contract_address
  ,b.name AS name
  ,b.symbol AS symbol
  ,b.royalty_pct
  ,'Decent.xyz' as platform
from {{ ref('decent_optimism_edition_metadata') }} as b
GROUP BY 1,2,3,4

UNION ALL

select
   b.edition_address as contract_address
  ,b.name AS name
  ,b.symbol AS symbol
  ,b.royalty_pct
  ,'Mirror' as platform
from {{ ref('mirror_optimism_edition_metadata') }} as b
GROUP BY 1,2,3,4
