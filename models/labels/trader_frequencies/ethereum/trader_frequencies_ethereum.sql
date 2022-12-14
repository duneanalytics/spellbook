{{
    config(
        alias='trader_frequencies_ethereum',
    )
}}

with
 trader_frequencies as (
    select
        taker as address,
        count(tx_hash) / datediff(max(block_time), min(block_time)) as trades_per_day
    from (
        select taker, block_time, tx_hash
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select taker, block_time, tx_hash
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    )
    group by taker
    -- That have at least more than 1 trade
    having datediff(max(block_time), min(block_time)) > 0
 )

select
  array("ethereum") as blockchain,
  address,
  case
    when trades_per_day >= 1 then 'Daily Trader'
    when trades_per_day >= 0.142857142857 then 'Weekly Trader'
    else 'Monthly Trader'
  end as name,
  "trader_frequencies" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-14') as created_at,
  now() as updated_at
from
  trader_frequencies
