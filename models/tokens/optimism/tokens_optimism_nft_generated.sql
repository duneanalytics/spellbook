 {{
  config(
    alias='tokens_optimism_nft_generated',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["msilb7"]\') }}'
  )
}}
select distinct
   b.edition_address as contract_address
  ,b.edition_name AS name
  ,b.edition_symbol AS symbol
  ,n.standard
from {{ ref('sound_xyz_optimism_edition_metadata') }} as b
left join {{ ref('tokens_ethereum_nft')}} as n
  on n.contract_address = b.edition_address
GROUP BY 1,2,3,4

-- UNION ALL
