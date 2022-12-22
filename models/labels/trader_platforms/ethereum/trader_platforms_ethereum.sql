{{
    config(
        alias='trader_platforms_ethereum',
    )
}}

with
 trader_platforms as (
    select
        taker as address,
        count(tx_hash) / datediff(max(block_date), min(block_date)) as trades_per_day
    from (
        select taker, block_date, tx_hash
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select taker, block_date, tx_hash
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    )
    group by taker
    -- That have at least more than 1 trade
    having datediff(max(block_date), min(block_date)) > 0
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
