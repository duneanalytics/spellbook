{{
    config(
        alias='trader_platforms_ethereum',
    )
}}

with
 trader_platforms as (
    select
        taker as address,
        project
    from (
        select taker, block_date, tx_hash
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select taker, block_date, tx_hash
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    )
 )

select
  array("ethereum") as blockchain,
  address,
  array_join(collect_set(concat(upper(substring(project,1,1)),substring(project,2))), ', ') ||' User' as name,
  "trader_platforms" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-21') as created_at,
  now() as updated_at
from
  trader_platforms
