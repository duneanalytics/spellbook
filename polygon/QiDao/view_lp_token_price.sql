BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao.view_lp_token_price;

CREATE MATERIALIZED VIEW qidao.view_lp_token_price as (
with token_symbols as (
  select distinct token_a_symbol as symbol from qidao.view_lp_basic_info where dex_name = 'QuickSwap'
  union all
  select distinct token_b_symbol as symbol from qidao.view_lp_basic_info where dex_name = 'QuickSwap'
)
,lp_address as (
  select lp_contract_address
  from qidao.view_lp_basic_info where dex_name = 'QuickSwap'
)
,token_prices as (
  select minute, symbol, price
  from prices.usd
  where minute >= '2021-5-2'
    and symbol in (select distinct symbol from token_symbols)
)
,token_prices_day as (
  select a."day", a."symbol", b."price"
  from (select date_trunc('day', minute) as day, symbol,
               max(minute) as minute from token_prices
        group by day, symbol order by day, symbol
       ) a inner join token_prices b
         on a."minute" = b."minute" and a."symbol" = b."symbol"
  order by 1, 2
)
,lp_reserve as (
  select *
  from (select evt_block_time, contract_address, reserve0, reserve1
        from quickswap."UniswapV2Pair_evt_Sync"
        where evt_block_time >= '2021-5-1'
          and contract_address in
              (select lp_contract_address from lp_address)
        union all
        select evt_block_time, contract_address, reserve0, reserve1
        from qidao."QIMaticLP_evt_Sync"
        where evt_block_time >= '2022-2-8'
       ) a
  order by evt_block_time, contract_address
)
,reserve_day_last as (
  select a."day", a."contract_address",
         avg(b."reserve0") as reserve0, avg(b."reserve1") as reserve1
  from (select date_trunc('day', evt_block_time) as day, contract_address,
        max(evt_block_time) as evt_block_time
        from lp_reserve group by day, contract_address
       ) a inner join lp_reserve b
       on a."evt_block_time" = b."evt_block_time"
       and a."contract_address" = b."contract_address"
  group by 1,2
  order by 1,2
)
,reserve_day_last_ext as (
  select day, contract_address, reserve0, reserve1,
         lead(day, 1, date_trunc('day', now()) + interval '1 day') over (partition by contract_address order by day) as next_day
  from reserve_day_last
)
,lp_changes as (
  select day, contract_address, sum(amount) as amount
  from (
    select date_trunc('day', evt_block_time) as day, contract_address,
           value / 1e18 as amount
    from erc20."ERC20_evt_Transfer"
    where evt_block_time >= '2021-5-2'
      and contract_address in
          (select lp_contract_address from lp_address)
      and "from" = '\x0000000000000000000000000000000000000000'
    union all
    select date_trunc('day', evt_block_time) as day, contract_address,
           (-1) * value / 1e18 as amount
    from erc20."ERC20_evt_Transfer"
    where evt_block_time >= '2021-5-2'
      and contract_address in
          (select lp_contract_address from lp_address)
      and "to" = '\x0000000000000000000000000000000000000000'
  ) a
  group by 1, 2
  order by 1, 2
)
,lp_supply as (
  select day, contract_address,
         sum(amount) over (partition by contract_address order by day) as supply,
         lead(day, 1, date_trunc('day', now()) + interval '1 day') over (partition by contract_address order by day) as next_day
  from lp_changes
)
,lp_price as (
  select a.day, c."lp_contract_address", c."lp_name",
        (d.reserve0 / (10^c."token_a_decimals") * a.price + d.reserve1 / (10^c."token_b_decimals") * b.price) / e.supply as price
  from token_prices_day a inner join token_prices_day b on a."day" = b."day"
       inner join qidao.view_lp_basic_info c
         on a."symbol" = c."token_a_symbol"
            and b."symbol" = c."token_b_symbol"
       inner join reserve_day_last_ext d
         on c."lp_contract_address" = d."contract_address"
            and a."day" >= d."day" and a."day" < d."next_day"
       inner join lp_supply e
         on c."lp_contract_address" = e."contract_address"
         and a."day" >= e."day" and a."day" < e."next_day"
  order by 1, 2, 3    
)
select * from (
select day, lp_contract_address as contract_address,
       lp_name as name, price
from lp_price
union all
select day,
       '\xa3fa99a148fa48d14ed51d610c367c61876997f1' as contract_address,
       'MAI' as name, price
from token_prices_day where symbol = 'MIMATIC'
union all
select day, 
       '\x447646e84498552e62eCF097Cc305eaBFFF09308' as contract_address,
       'MAI+3Pool3CRV-f' as name, price
from token_prices_day where symbol = 'USDC' -- just take usdc price
) a
order by day, name
);

CREATE UNIQUE INDEX IF NOT EXISTS qidao_view_lp_token_price_idx ON qidao.view_lp_token_price (day, name);

INSERT INTO cron.job(schedule, command)
VALUES ('0 */2 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_lp_token_price$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;