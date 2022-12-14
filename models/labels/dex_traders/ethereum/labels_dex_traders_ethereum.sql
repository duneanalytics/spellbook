{{config(alias='dex_traders_ethereum')}}

with 
 dex_traders as (
    select distinct taker as address
    from {{ref('dex_trades')}}
    where blockchain = 'ethereum'
  )
select
  array("ethereum") as blockchain,
  address,
  "DEX Trader" AS name,
  "dex_traders" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-14') as created_at,
  now() as updated_at
from
  dex_traders