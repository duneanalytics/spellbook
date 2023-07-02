 {{
  config(
    tags = ['dunesql']
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
  ,'Sound.xyz' as platform
from {{ ref('sound_xyz_optimism_edition_metadata') }} as b
GROUP BY 1,2,3

UNION ALL

select
   b.edition_address as contract_address
  ,b.edition_name AS name
  ,b.edition_symbol AS symbol
  ,'Zora' as platform
from {{ ref('zora_optimism_edition_metadata') }} as b
GROUP BY 1,2,3
