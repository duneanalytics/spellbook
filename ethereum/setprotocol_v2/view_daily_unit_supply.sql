create or replace view setprotocol_v2.view_daily_unit_supply as

with set_balance_changes as (
  -- mints
  select contract_address as set_address
    , value * 1.0 / 10 ^ 18 as quantity
    , evt_block_time as timestamp
  from setprotocol_v2."SetToken_evt_Transfer"
  where "from" = '\x0000000000000000000000000000000000000000'
  union all

  -- redeems
  select contract_address as set_address
    , value * -1.0 / 10 ^ 18 as quantity
    , evt_block_time as timestamp
  from setprotocol_v2."SetToken_evt_Transfer"
  where "to" = '\x0000000000000000000000000000000000000000'
)
, net_balance_changes as (
  select date_trunc('day', timestamp) as day
    , set_address
    , sum(quantity) as net_quantity_change
  from set_balance_changes
  group by 1,2
)
, day_series as (
  select generate_series(min(day), now(), '1 day') AS day
  from net_balance_changes
)
, daily_balances as (
  select d.day
    , nbc.set_address
    , sum(net_quantity_change) as supply
  from day_series d
  left join net_balance_changes nbc on nbc.day <= d.day
  group by 1,2
  order by 2,1
)

select * from daily_balances
;