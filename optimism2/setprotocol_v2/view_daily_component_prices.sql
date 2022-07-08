-- Pull prices from approx_prices_from_dex_data
-- Note that this pulls from erc20.tokens, so if the token is not in there then we won't have the price

create or replace view setprotocol_v2.view_daily_component_prices as 

with initial_components as (
  -- Get the initial components from the create function
  select output_0 as set_address
    , unnest(_components) as component_address
    , unnest(_units) as unit
    , call_block_time as timestamp
    , call_block_time::date as day
  from setprotocol_v2."SetTokenCreator_call_create"
  where call_success is true
)
, all_components as (
  select distinct component_address
  from initial_components
  union
  select distinct _component as component_address
  from setprotocol_v2."SetToken_evt_ExternalPositionUnitEdited"
  union
  select distinct _component
  from setprotocol_v2."SetToken_evt_DefaultPositionUnitEdited"
)
, weth_prices as (
    select p.contract_address
        , p.hour::date as day
        , avg(median_price) as weth_usd_price -- I know taking mean of median is silly but it's what we got
    from prices.approx_prices_from_dex_data p
    where p.contract_address = '\x4200000000000000000000000000000000000006' -- wETH
    group by 1,2
)
, usd_prices as (
    select ac.component_address
      -- , p.contract_address
      , p.symbol
      -- , p.decimals
      , p.hour::date as day
      , avg(median_price) as price_usd
    from all_components ac
    inner join prices.approx_prices_from_dex_data p on ac.component_address = p.contract_address
    group by 1,2,3
)
, component_prices as (
    select p.component_address
        , symbol
        , p.day
        , price_usd
        , price_usd / weth_usd_price as price_eth
    from usd_prices p
    inner join weth_prices wp on p.day = wp.day
)
select * from component_prices;
