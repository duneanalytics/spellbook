-- test query here: https://dune.xyz/queries/594861
-- This insert query is modeled off of insert_prices_from_dex_data: 
-- https://github.com/duneanalytics/abstractions/blob/master/ethereum/prices/insert_prices_from_dex_data.sql
CREATE OR REPLACE FUNCTION setprotocol_v2.insert_daily_component_prices(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
-- CREATE OR REPLACE FUNCTION dune_user_generated.insert_daily_component_prices(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

--Step 1: Grab components from coinpaprika
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
  from setprotocol_v2."SetToken_evt_ComponentAdded"
)
, components_mapped as ( -- don't even bother checking if the pre-mapped components have prices - use the map directly
  select coalesce(m.mapped_component_address, ac.component_address) as mapped_component_address
    , m.symbol as pre_mapped_symbol
    , ac.component_address
  from all_components ac
  left join setprotocol_v2.set_component_token_mappings m on ac.component_address = m.component_address
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
  where p.minute >= start_time
   and p.minute < end_time
  group by 1,2,3
)
, daily_eth_price_usd as (
  select minute::date as date
    , avg(price) as eth_price
  from prices.layer1_usd p
  where p.minute >= start_time
   and p.minute < end_time
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
, rows as (
  insert into setprotocol_v2.daily_component_prices (
  -- insert into dune_user_generated.daily_component_prices (
    component_address
    , symbol
    , date
    , data_source
    , avg_price_usd
    , eth_price
    , avg_price_eth
  )
  select
    component_address
    , symbol
    , date
    , data_source
    , avg_price_usd
    , eth_price
    , avg_price_eth
  from paprika_price_feed

  on CONFLICT(component_address, date) do update set 
    avg_price_usd = EXCLUDED.avg_price_usd
    , eth_price = EXCLUDED.eth_price
    , avg_price_eth = EXCLUDED.avg_price_eth
  RETURNING 1
  
)
SELECT count(*) INTO r from rows;
-------------------------------------------------------------------------------------------------------------------------
--Step 2: Grab components from dex trades
with initial_components as (
  -- Get the initial components from the create function
  select output_0 as set_address
    , unnest(_components) as component_address
    , unnest(_units) as unit
    , call_block_time as timestamp
    , call_block_time::date as day
  from setprotocol_v2."SetTokenCreator_call_create"
)
, all_components as (
  select distinct component_address
  from initial_components
  union
  select distinct _component as component_address
  from setprotocol_v2."SetToken_evt_ComponentAdded"
)
, components_mapped as ( -- don't even bother checking if the pre-mapped components have prices - use the map directly
  select coalesce(m.mapped_component_address, ac.component_address) as mapped_component_address
    , m.symbol as pre_mapped_symbol
    , ac.component_address
  from all_components ac
  left join setprotocol_v2.set_component_token_mappings m on ac.component_address = m.component_address
)
, tokens_from_paprika as (
  select distinct contract_address 
  from prices.usd p 
  where p.minute >= start_time
   and p.minute < end_time
)
, missing_components_mapped as (
  select ac.component_address
    , ac.mapped_component_address
    , ac.pre_mapped_symbol
  from components_mapped ac
  left join tokens_from_paprika tfp on ac.mapped_component_address = tfp.contract_address
  where tfp.contract_address is null
)
-- The insertion operation is broken up into multiple queries that "forget" the last known price
-- so we should re-introduce the "last known price" from the table so it can be used in imputations
, anchor_prices as (
  select dcp.date
    , dcp.component_address
    , dcp.symbol
    , dcp.avg_price_usd as avg_price
  from setprotocol_v2.daily_component_prices dcp
  -- from dune_user_generated.daily_component_prices dcp
  inner join missing_components_mapped mc on dcp.component_address = mc.component_address
  where dcp.date = start_time::date - interval '1 day'
)
, daily_component_prices_usd_passing as (
  select date
    , component_address
    , symbol
    , avg_price
  from anchor_prices
  union
  select p.hour::date as date
    , mc.component_address
    , coalesce(mc.pre_mapped_symbol, p.symbol) as symbol
    -- , sum(sample_size) as daily_samples
    , percentile_disc(0.5) within group (order by median_price) as avg_price -- use the median price for the day to remove outliers
    -- , sum(median_price* sample_size) / sum(sample_size) as avg_price -- sample size weighted average median price
  from prices.prices_from_dex_data p
  inner join missing_components_mapped mc on p.contract_address = mc.mapped_component_address
  where p.hour >= start_time
   and p.hour < end_time
    and sample_size > 0
  group by 1,2,3
  having sum(sample_size) > 5 -- minimum of 6 samples required to set a daily price
     and count(distinct median_price) > 5 -- minimum of 6 unique rows required
)
, daily_component_prices_usd_passing_lead as (
  select date
        , component_address
        , symbol
        , avg_price
        , lead(date, 1) over (partition by component_address order by date) as next_date
            -- this gives the day that this particular snapshot value is valid until
    from daily_component_prices_usd_passing 
)
, day_series as (
  SELECT generate_series(min(date), now(), '1 day') AS day 
        FROM daily_component_prices_usd_passing
)
, imputed_component_prices_usd as (
    select d.day as date
        , p.component_address
        , p.symbol
        , p.avg_price as avg_price_usd
    from day_series d 
    inner join daily_component_prices_usd_passing_lead p
        on d.day >= p.date
        and d.day < coalesce(p.next_date,now()::date + 1) -- if it's missing that means it's the last entry in the series
)
, daily_eth_price_usd as (
  select minute::date as date
    , avg(price) as eth_price
  from prices.layer1_usd p
  where p.minute >= start_time
   and p.minute < end_time
  and symbol = 'ETH'
  group by 1
)
, dex_price_feed as (
  select p.component_address
    , p.symbol
    , p.date
    , 'prices.prices_from_dex_data' as data_source
    , p.avg_price_usd
    , e.eth_price
    , p.avg_price_usd / e.eth_price as avg_price_eth
  from imputed_component_prices_usd p
  inner join daily_eth_price_usd e on p.date = e.date
)
, rows as (
  insert into setprotocol_v2.daily_component_prices (
  -- insert into dune_user_generated.daily_component_prices (
    component_address
    , symbol
    , date
    , data_source
    , avg_price_usd
    , eth_price
    , avg_price_eth
  )
  select
    component_address
    , symbol
    , date
    , data_source
    , avg_price_usd
    , eth_price
    , avg_price_eth
  from dex_price_feed

  on CONFLICT(component_address, date) do update set 
    avg_price_usd = EXCLUDED.avg_price_usd
    , eth_price = EXCLUDED.eth_price
    , avg_price_eth = EXCLUDED.avg_price_eth
  RETURNING 1
  
)
SELECT count(*) + r INTO r from rows;
-------------------------------------------------------------------------------------------------------------------------
-- Step 3: Update prices for cETH using Compound-specific query
-- Compound Protocol V2: exhcangeRate (https://compound.finance/docs#protocol-math)
-- one_ceth_in_eth is strictly increasing over time
with ceth_eth_exchange_rate as (
    select output_0 as exchange_rate_raw
      , output_0 / (1.0 * 10 ^ (18 + 18 - 8)) as one_ceth_in_eth
      , call_block_time
    from compound_v2."cEther_call_exchangeRateCurrent"
    where call_success
    and call_block_time >= start_time
    and call_block_time < end_time
)
, day_series as (
  select generate_series(start_time::date - interval '1 day' -- include an extra day for the anchor price
                        , end_time::date, '1 day') as day
)
, anchor_prices as (
    select dcp.date as day
    -- , dcp.component_address
    -- , dcp.symbol
    , dcp.avg_price_eth as one_ceth_in_eth
  from setprotocol_v2.daily_component_prices dcp
  -- from dune_user_generated.daily_component_prices dcp
  where dcp.date = start_time::date - interval '1 day'
    and dcp.component_address = '\x4ddc2d193948926d02f9b1fe9e1daa0718270ed5'::bytea -- cETH address
)
, avg_daily_ceth_eth_exchange_rate as (
    select day
        , one_ceth_in_eth
    from anchor_prices
    union
    select call_block_time::date as day
      , avg(one_ceth_in_eth) as one_ceth_in_eth
    from ceth_eth_exchange_rate
    group by call_block_time::date
)
-- Impute daily cETH to ETH exchange rate with the last known value
-- Becuase the exchange rate is strictly increasing, this results in a slight price underestimate
-- when cEther_call_exchangeRateCurrent data availability is low
, avg_daily_ceth_eth_exchange_rate_imputed as (
    select d.day
      , max(r.one_ceth_in_eth) over (order by d.day) as one_ceth_in_eth -- this only works because the exchange rate is strictly increasing
    from day_series d
    left join avg_daily_ceth_eth_exchange_rate r on d.day = r.day
)
, avg_daily_eth_price as (
    select minute::date as day
      , avg(price) as eth_price
    from prices.layer1_usd
    where symbol = 'ETH'
    and minute >= start_time
    and minute < end_time
    group by minute::date
)
, avg_daily_ceth_price as (
    select '\x4ddc2d193948926d02f9b1fe9e1daa0718270ed5'::bytea as component_address -- cETH address
        , 'cETH' as symbol
        , r.day as date
        , 'ceth_feed' as data_source
      , r.one_ceth_in_eth * p.eth_price as avg_price_usd
      , p.eth_price as eth_price
      , r.one_ceth_in_eth as avg_price_eth
      
    from avg_daily_ceth_eth_exchange_rate_imputed r
    inner join avg_daily_eth_price p on r.day = p.day
)
, rows as (
  insert into setprotocol_v2.daily_component_prices (
  -- insert into dune_user_generated.daily_component_prices (
    component_address
    , symbol
    , date
    , data_source
    , avg_price_usd
    , eth_price
    , avg_price_eth
  )
  select
    component_address
    , symbol
    , date
    , data_source
    , avg_price_usd
    , eth_price
    , avg_price_eth
  from avg_daily_ceth_price

  on CONFLICT(component_address, date) do update set 
    data_source = EXCLUDED.data_source
    , avg_price_usd = EXCLUDED.avg_price_usd
    , eth_price = EXCLUDED.eth_price
    , avg_price_eth = EXCLUDED.avg_price_eth
  RETURNING 1
)
SELECT count(*) + r INTO r from rows;
-------------------------------------------------------------------------------------------------------------------------
-- Step 4: Update prices for cWBTC (and other cTokens in the future) using Compound-specific query
-- exchangeRate (https://compound.finance/docs#protocol-math)
-- one_cWBTC_in_WBTC is strictly increasing over time
with components as (
  -- this is the list of components that are pegged to wBTC
  select '\xc11b1268c1a384e55c48c2391d8d480264a3a7f4'::bytea as component_address -- cWBTC address
  union
  select '\xccF4429DB6322D5C611ee964527D42E5d685DD6a'::bytea as component_addrses -- cWBTC2, erc20Delegatorcall
)
, cwbtc_wbtc_exchange_rate as (
    select contract_address
      , output_0 as exchange_rate_raw
      , output_0 / (1.0 * 10 ^ (18 + 8 - 8)) as one_cwbtc_in_wbtc
      , call_block_time
    from compound_v2."cErc20_call_exchangeRateCurrent"
    inner join components c on contract_address = component_address
    where call_success 
    -- and contract_address = '\xc11b1268c1a384e55c48c2391d8d480264a3a7f4' -- cWBTC address
    and call_block_time >= start_time
    and call_block_time < end_time
    union
    select contract_address
      , output_0 as exchange_rate_raw
      , output_0 / (1.0 * 10 ^ (18 + 8 - 8)) as one_cwbtc_in_wbtc
      , call_block_time
    from compound_v2."CErc20Delegator_call_exchangeRateCurrent"
    inner join components c on contract_address = component_address
    where call_success 
    -- and contract_address = '\xccF4429DB6322D5C611ee964527D42E5d685DD6a' -- cWBTC address
    and call_block_time >= start_time
    and call_block_time < end_time
)
, day_series as (
  select generate_series(start_time::date - interval '1 day' -- include an extra day for the anchor price
                        , end_time::date, '1 day') as day
)
, avg_daily_wbtc_price as (
    select minute::date as day
      , avg(price) as wbtc_price
    from prices.usd
    where contract_address = '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' -- WBTC contract address
    and minute >= start_time - interval '1 day' -- include one extra day for the anchor price
    and minute < end_time
    group by minute::date
)
, avg_daily_eth_price as (
    select minute::date as day
      , avg(price) as eth_price
    from prices.layer1_usd
    where symbol = 'ETH'
    and minute >= start_time
    and minute < end_time
    group by minute::date
)
, anchor_prices as (
    select dcp.date as day
     , dcp.component_address
    -- , dcp.symbol
    , dcp.avg_price_usd / pw.wbtc_price as one_cwbtc_in_wbtc
  from setprotocol_v2.daily_component_prices dcp
  -- from dune_user_generated.daily_component_prices dcp
  inner join avg_daily_wbtc_price pw on dcp.date = pw.day
  inner join components c on dcp.component_address = c.component_address
  where dcp.date = start_time::date - interval '1 day'
  --  and dcp.component_address = '\xc11b1268c1a384e55c48c2391d8d480264a3a7f4'::bytea -- cWBTC address
)
, avg_daily_cwbtc_wbtc_exchange_rate as (
    select day
        , component_address
        , one_cwbtc_in_wbtc
    from anchor_prices
    union
    select call_block_time::date as day
      , contract_address as component_address
      , avg(one_cwbtc_in_wbtc) as one_cwbtc_in_wbtc
    from cwbtc_wbtc_exchange_rate
    group by 1,2
)
, avg_daily_cwbtc_wbtc_exchange_rate_lead as (
  select day
        , component_address
        , one_cwbtc_in_wbtc
        , lead(day, 1) over (partition by component_address order by day) as next_day
            -- this gives the day that this particular snapshot value is valid until
    from avg_daily_cwbtc_wbtc_exchange_rate 
)
, avg_daily_cwbtc_wbtc_exchange_rate_imputed as (
    select d.day
        , p.component_address
        , p.one_cwbtc_in_wbtc
    from day_series d 
    inner join avg_daily_cwbtc_wbtc_exchange_rate_lead p
        on d.day >= p.day
        and d.day < coalesce(p.next_day,now()::date + 1) -- if it's missing that means it's the last entry in the series
)
, avg_daily_cwbtc_price as (
    select r.component_address 
        , 'cWBTC' as symbol
        , r.day as date
        , 'cwbtc_feed' as data_source
      -- , r.one_cwbtc_in_wbtc
      -- , p.wbtc_price
      , r.one_cwbtc_in_wbtc * pb.wbtc_price as avg_price_usd
      , pe.eth_price
      , r.one_cwbtc_in_wbtc * pb.wbtc_price / pe.eth_price as avg_price_eth
    from avg_daily_cwbtc_wbtc_exchange_rate_imputed r
    inner join avg_daily_wbtc_price pb on r.day = pb.day
    inner join avg_daily_eth_price pe on r.day = pe.day
    where r.one_cwbtc_in_wbtc is not null
)
, rows as (
  insert into setprotocol_v2.daily_component_prices (
  -- insert into dune_user_generated.daily_component_prices (
    component_address
    , symbol
    , date
    , data_source
    , avg_price_usd
    , eth_price
    , avg_price_eth
  )
  select
    component_address
    , symbol
    , date
    , data_source
    , avg_price_usd
    , eth_price
    , avg_price_eth
  from avg_daily_cwbtc_price

  on CONFLICT(component_address, date) do update set 
    data_source = EXCLUDED.data_source
    , avg_price_usd = EXCLUDED.avg_price_usd
    , eth_price = EXCLUDED.eth_price
    , avg_price_eth = EXCLUDED.avg_price_eth
  RETURNING 1
)
SELECT count(*) + r INTO r from rows;
RETURN r;
END
$function$;

-- half-yearly backfill starting '2020-09-10', the date of the first Set contract deployment
SELECT setprotocol_v2.insert_daily_component_prices('2020-09-10', '2021-01-01')
WHERE NOT EXISTS (SELECT * FROM setprotocol_v2.daily_component_prices WHERE date >= '2020-09-10' and date < '2021-01-01');

SELECT setprotocol_v2.insert_daily_component_prices('2021-01-01', '2021-06-01')
WHERE NOT EXISTS (SELECT * FROM setprotocol_v2.daily_component_prices WHERE date >= '2021-01-01' and date < '2021-06-01');

SELECT setprotocol_v2.insert_daily_component_prices('2021-06-01', '2022-01-01')
WHERE NOT EXISTS (SELECT * FROM setprotocol_v2.daily_component_prices WHERE date >= '2021-06-01' and date < '2022-01-01');

SELECT setprotocol_v2.insert_daily_component_prices('2022-01-01', now());

-- Have the insert script run once a day 20 minutes after midnight and noon
-- `start-time` is set to go back three days in time so that entries can be retroactively updated 
-- in case `dex.trades` or price data falls behind.
INSERT INTO cron.job (schedule, command)
VALUES ('20 0,12 * * *', $$
    SELECT setprotocol_v2.insert_daily_component_prices(
        (SELECT date_trunc('day', now()) - interval '3 days'),
        (SELECT date_trunc('day', now()) + interval '1 day')); -- the insert function is noninclusive of the end date, so add a day
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

/*
use the following code to test the script
SELECT dune_user_generated.insert_daily_component_prices('2020-09-10', '2021-01-01')
WHERE NOT EXISTS (SELECT * FROM dune_user_generated.daily_component_prices WHERE date >= '2020-09-10' and date < '2021-01-01');

SELECT dune_user_generated.insert_daily_component_prices('2021-01-01', '2021-06-01')
WHERE NOT EXISTS (SELECT * FROM dune_user_generated.daily_component_prices WHERE date >= '2021-01-01' and date < '2021-06-01');

SELECT dune_user_generated.insert_daily_component_prices('2021-06-01', '2022-01-01')
WHERE NOT EXISTS (SELECT * FROM dune_user_generated.daily_component_prices WHERE date >= '2021-06-01' and date < '2022-01-01');

SELECT dune_user_generated.insert_daily_component_prices('2022-01-01', now());

*/