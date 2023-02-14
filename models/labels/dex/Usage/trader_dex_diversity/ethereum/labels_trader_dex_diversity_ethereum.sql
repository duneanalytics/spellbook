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
  "ethereum" as blockchain,
  address,
  concat('Number of DEXs traded on: ', dex_diversity) as name,
  "dex" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-15') as created_at,
  now() as updated_at,
  "trader_dex_diversity" as model_name,
  "usage" as label_type
from
  trader_dex_diversity
