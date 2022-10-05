CREATE OR REPLACE FUNCTION aztec_v2.insert_daily_token_prices(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
-- CREATE OR REPLACE FUNCTION dune_user_generated.aztec_v2_insert_daily_token_prices(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

---------------------------------------------------------------------
-- Section 1: which tokens we're looking at
with tokens as (
  -- Get the relevant components
  select distinct contract_address as token_address
  from aztec_v2.view_rollup_bridge_transfers
  -- from dune_user_generated.aztec_v2_rollup_bridge_transfers
  where contract_address <> '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::bytea -- exclude ETH from the initial price feed
)
, tokens_from_paprika as (
  select distinct contract_address as token_address
  from prices.usd p 
  where p.minute >= start_time::date
    and p.minute < end_time::date
)
, missing_tokens as (
  -- for tokens that are missing from coinpaprika we grab dex prices
  select t.token_address
  from tokens t
  left join tokens_from_paprika tfp on t.token_address = tfp.token_address
  where tfp.token_address is null
)
---------------------------------------------------------------------
-- Section 2: CoinPaprika Price Feed
, daily_token_prices_usd_paprika as (
  select t.token_address
    , p.symbol
    , p.minute::date as date
    , avg(price) as avg_price_usd
  from tokens t
  inner join prices.usd p on t.token_address = p.contract_address
  where p.minute >= start_time::date
    and p.minute < end_time::date 
  group by 1,2,3
)
----------------------------------------------------------------------
-- Section 3: Dex Price Feed
, anchor_prices as (
  -- this part of the query gets anchor prices for imputation purposes
  select dcp.date
    , dcp.token_address
    , dcp.symbol
    , dcp.avg_price_usd as avg_price
  from aztec_v2.daily_token_prices dcp
  -- from dune_user_generated.aztec_v2_daily_token_prices dcp
  inner join missing_tokens mt on dcp.token_address = mt.token_address
  where dcp.date = start_time::date - interval '1 day'
)
, daily_token_prices_usd_dex_passing as (
  -- this part of the query applies some data quality standards to the prices_from_dex data
  -- to try and smooth out extreme price movements from illiquid DEXes
  select date
    , token_address
    , symbol
    , avg_price
  from anchor_prices
  union
  select p.hour::date as date
    , mt.token_address
    , p.symbol
    -- , sum(sample_size) as daily_samples
    , percentile_disc(0.5) within group (order by median_price) as avg_price -- use the median price for the day to remove outliers
    -- , sum(median_price* sample_size) / sum(sample_size) as avg_price -- sample size weighted average median price
  from prices.prices_from_dex_data p
  inner join missing_tokens mt on p.contract_address = mt.token_address
  where p.hour >= start_time::date
    and p.hour < end_time::date
    and sample_size > 0
  group by 1,2,3
  having sum(sample_size) > 5 -- minimum of 6 samples required to set a daily price
     and count(distinct median_price) > 5 -- minimum of 6 unique rows required
)
, daily_token_prices_usd_dex_passing_lead as (
  select date
        , token_address
        , symbol
        , avg_price
        , lead(date, 1) over (partition by token_address order by date) as next_date
            -- this gives the day that this particular snapshot value is valid until
    from daily_token_prices_usd_dex_passing 
)
, day_series as (
  SELECT generate_series(min(date), now(), '1 day') AS day 
        FROM daily_token_prices_usd_dex_passing
)
, imputed_token_prices_dex_usd as (
    select d.day as date
        , p.token_address
        , p.symbol
        , p.avg_price as avg_price_usd
    from day_series d 
    inner join daily_token_prices_usd_dex_passing_lead p
        on d.day >= p.date
        and d.day < coalesce(p.next_date,now()::date + 1) -- if it's missing that means it's the last entry in the series
)
-------------------------------------------------------------------------------------------------------------------
-- Section 4: Synthesize with ETH price
, daily_eth_price_usd as (
  select minute::date as date
    , avg(price) as eth_price
  from prices.layer1_usd p
  where p.minute >= start_time::date
    and p.minute < end_time::date
  and symbol = 'ETH'
  group by 1
)
, paprika_price_feed as (
  select p.token_address
    , p.symbol
    , p.date
    , 'prices.usd' as data_source
    , p.avg_price_usd
    , e.eth_price
    , p.avg_price_usd / e.eth_price as avg_price_eth
  from daily_token_prices_usd_paprika p
  inner join daily_eth_price_usd e on p.date = e.date
)
, dex_price_feed as (
  select p.token_address
    , p.symbol
    , p.date
    , 'prices.prices_from_dex_data' as data_source
    , p.avg_price_usd
    , e.eth_price
    , p.avg_price_usd / e.eth_price as avg_price_eth
  from imputed_token_prices_dex_usd p
  inner join daily_eth_price_usd e on p.date = e.date
)
, rows as (
  insert into aztec_v2.daily_token_prices (
  -- insert into dune_user_generated.aztec_v2_daily_token_prices (
      token_address 
    , symbol 
    , date 
    , data_source 
    , avg_price_usd 
    , eth_price 
    , avg_price_eth 
  )
  select * from paprika_price_feed
  union 
  select * from dex_price_feed
  union
  select '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'::bytea as token_address
    , 'ETH' as symbol
    , date
    , 'prices.layer1_usd' as data_source
    , eth_price as avg_price_usd
    , eth_price
    , 1 as avg_price_eth
  from daily_eth_price_usd
  
  on conflict(token_address, date) do update set
    data_source = excluded.data_source
    , avg_price_usd = EXCLUDED.avg_price_usd
    , eth_price = EXCLUDED.eth_price
    , avg_price_eth = EXCLUDED.avg_price_eth
  returning 1
)
select count(*) into r from rows;

RETURN r;
END
$function$;

-- truncate the table before backfilling everything
truncate table aztec_v2.daily_token_prices;


-- backfill starting '2022-05-13'
SELECT aztec_v2.insert_daily_token_prices('2022-05-13', now());
-- select dune_user_generated.aztec_v2_insert_daily_token_prices('2022-05-13', now());

-- Have the insert script run four times a day
-- `start-time` is set to go back three days in time so that entries can be retroactively updated 
-- in case `dex.trades` or price data falls behind.
INSERT INTO cron.job (schedule, command)
VALUES ('0 0,6,12,18 * * *', $$
    SELECT aztec_v2.insert_daily_token_prices(
        (SELECT date_trunc('day', now()) - interval '3 days'),
        (SELECT date_trunc('day', now()) + interval '1 day')); -- the insert function is noninclusive of the end date, so add a day
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;