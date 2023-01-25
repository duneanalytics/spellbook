{{
    config(
        alias='trader_dex_diversity_ethereum',
    )
}}

with
 trader_dex_diversity as (
    select
        count(distinct project) as dex_diversity,
        taker as address
    from (
        select taker, project
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select taker, project
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    )
    group by taker
 )

select
  array("ethereum") as blockchain,
  address,
  concat('Number of DEXs traded on: ', dex_diversity) as name,
  "trader_dex_diversity" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-15') as created_at,
  now() as updated_at
from
  trader_dex_diversity
