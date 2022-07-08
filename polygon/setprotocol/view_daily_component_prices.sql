-- Pull prices from prices.usd
-- Note that this pulls from erc20.tokens, so if the token is not in there then we won't have the price

create or replace view setprotocol.view_daily_component_prices as 

with initial_components as (
  -- Get the initial components from the create function
  select output_0 as set_address
    , unnest(_components) as component_address
    , unnest(_units) as unit
    , call_block_time as timestamp
    , call_block_time::date as day
  from setprotocol."SetTokenCreator_call_create"
  inner join setprotocol.view_significant_sets ss on output_0 = ss.set_address
  where call_success is true
)
, all_components as (
  select distinct component_address
  from initial_components
  union
  select distinct _component as component_address
  from setprotocol."SetToken_evt_ComponentAdded"
  inner join setprotocol.view_significant_sets ss on "contract_address" = ss.set_address
)
, components_mapped as ( -- don't even bother checking if the pre-mapped components have prices - use the map directly
  select coalesce(m.mapped_component_address, ac.component_address) as mapped_component_address
    , m.symbol as pre_mapped_symbol
    , ac.component_address
  from all_components ac
  left join setprotocol.token_mappings m on ac.component_address = m.component_address
)
, daily_component_prices_usd as (
  -- so we're taking the average of a time series here, which is not fully valid, but this part of the query was benchmarked to 3 minutes
  -- whereas if we were to take the open instead, that gets a 4.5x runtime of 13 minutes, so I think taking the average
  -- is a reasonable approximation for the performance we get.
  select ac.component_address
    , upper(coalesce(ac.pre_mapped_symbol, p.symbol)) as symbol -- have to force all caps because prices.usd.symbol is inconsistently capitalized
    , p.minute::date as date
    , avg(price) as avg_price_usd
  from components_mapped ac
  inner join prices.usd p on ac.mapped_component_address = p.contract_address
  where p.minute >= '2021-07-09'::date -- first Set contract deployed on Polygon
  -- and p.minute <= '2022-03-16'::date -- end of the cache
  group by 1,2,3
)
, daily_eth_price_usd as (
  select minute::date as date
    , avg(price) as eth_price
  from prices.layer1_usd p
  where p.minute >= '2021-07-09'::date -- first Set contract deployed on Polygon
  -- and p.minute <= '2022-03-31'::date -- end of the cache
  and symbol = 'ETH'
  group by 1
)
, paprika_price_feed as (
  select p.component_address
    , p.symbol
    , p.date
    , 'prices.usd' as data_source
    , p.avg_price_usd
    , e.eth_price
    , p.avg_price_usd / e.eth_price as avg_price_eth
  from daily_component_prices_usd p
  inner join daily_eth_price_usd e on p.date = e.date
)
select * from paprika_price_feed
;