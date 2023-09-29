{{
    config(
        tags=['dunesql', 'prod_exclude'],
        alias = alias('trader_portfolios_ethereum'),
    )
}}

with
 trader_portfolios as (
    select
        sum(amount_usd) as portfolio_value_usd,
        wallet_address as address
    from {{ ref('balances_ethereum_erc20_day') }}
    where blockchain = 'ethereum' and day = CURRENT_DATE and amount > 0
    and wallet_address in (
        select distinct taker
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select distinct taker
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    )
    group by wallet_address
    -- For some reason there's negative portfolio values in here
    having portfolio_value_usd > 0
 )

select
  'ethereum' as blockchain,
  address,
  case
    when portfolio_value_usd > 90000 then '>$90k portfolio value'
    when portfolio_value_usd > 7000 then '$7k-$90k portfolio value'
    when portfolio_value_usd > 2000 then '$2k-$7k portfolio value'
    when portfolio_value_usd > 400 then '$400-$2k portfolio value'
    when portfolio_value_usd > 100 then '$100-$400 portfolio value'
    else '<=$100 portfolio value'
  end as name,
  'dex' AS category,
  'gentrexha' AS contributor,
  'query' AS source,
  timestamp '2022-12-15' as created_at,
  now() as updated_at,
  'trader_portfolios' as model_name,
  'usage' as label_type
from
  trader_portfolios
