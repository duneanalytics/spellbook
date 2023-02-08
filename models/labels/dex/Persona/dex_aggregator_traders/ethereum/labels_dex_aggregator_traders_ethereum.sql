{{config(alias='dex_aggregator_traders_ethereum')}}

with
 dex_traders as (
    select distinct taker as address
    from {{ref('dex_aggregator_trades')}}
    where blockchain = 'ethereum'
  )
select
  array("ethereum") as blockchain,
  address,
  "DEX Aggregator Trader" AS name,
  "dex_aggregator_traders" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-14') as created_at,
  now() as updated_at
from
  dex_traders