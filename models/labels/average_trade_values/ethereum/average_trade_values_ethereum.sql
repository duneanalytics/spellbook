{{
    config(
        alias='average_trade_values_ethereum',
    )
}}

with
 average_trade_values as (
    select
        avg(amount_usd) as average_trade_value,
        taker as address
    from (
        select taker, amount_usd
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select taker, amount_usd
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    )
    group by taker
 )

select
  array("ethereum") as blockchain,
  address,
  case
    when average_trade_value > 50000 then '>$50k avg. DEX trade value'
    when average_trade_value > 10000 then '$10k-$50k avg. DEX trade value'
    when average_trade_value > 5000 then '$5k-$10k avg. DEX trade value'
    when average_trade_value > 2000 then '$2k-$5k avg. DEX trade value'
    when average_trade_value > 1000 then '$1k-$2k avg. DEX trade value'
    when average_trade_value > 400 then '$400-$1k avg. DEX trade value'
    else '<=$400 avg. DEX trade value'
  end as name,
  "average_trade_values" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-12-15') as created_at,
  now() as updated_at
from
  average_trade_values
