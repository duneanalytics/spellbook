{{
    config(
        alias='trader_platforms_ethereum',
    )
}}

with
 trader_platforms as (
    select
        taker as address,
        block_time,
        project
    from (
        select taker, project, block_time
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select taker, project, block_time
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    )
    order by block_time
 )

select
  array("ethereum") as blockchain,
  address,
  array_join(array_distinct(collect_list(concat(upper(substring(project,1,1)),substring(project,2)))), ', ') ||' User' as name,
  "trader_platforms" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-21') as created_at,
  now() as updated_at
from
  trader_platforms
where address is not null
group by address
