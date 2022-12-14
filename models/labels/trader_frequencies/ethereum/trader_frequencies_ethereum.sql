{{
    config(
        alias='trader_frequencies_ethereum',
    )
}}

with
 trader_frequencies as (
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
  case
    when trades_per_day >= 1 then 'Daily Trader'
    when trades_per_day >= 0.142857142857 then 'Weekly Trader'
    when trades_per_day >= 0.0333333333333 then 'Monthly Trader'
    when trades_per_day >= 0.0027397260274 then 'Yearly Trader'
    else 'Sparse Trader'
  end as name,
  "trader_frequencies" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-14') as created_at,
  now() as updated_at
from
  trader_frequencies
