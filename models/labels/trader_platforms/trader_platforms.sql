{{
    config(
        alias='trader_platforms',
        post_hook='{{ expose_spells(\'
        ["ethereum", "fantom", "arbitrum", "avalanche_c", "gnosis", "bnb", "optimism", "polygon"]\', 
        "sector", 
        "labels", 
        \'["gentrexha"]\') }}'
    )
}}

with
 trader_platforms as (
    select
        taker as address,
        block_time,
        project,
        blockchain
    from (
        select blockchain, taker, project, block_time
        from {{ ref('dex_aggregator_trades') }}
        UNION ALL
        select blockchain, taker, project, block_time
        from {{ ref('dex_trades') }}
    )
    order by block_time
 )

select
  collect_list(blockchain) as blockchain,
  address,
  array_join(array_distinct(collect_list(initcap(project)), ', ')) ||' User' as name,
  "trader_platforms" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-21') as created_at,
  now() as updated_at
from
  trader_platforms
where address is not null
group by address