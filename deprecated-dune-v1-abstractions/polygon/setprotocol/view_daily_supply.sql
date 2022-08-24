-- this could use the significant sets view for DRY but it's faster this way, since
-- the rest of the query references CTEs within significant sets
create or replace view setprotocol.view_daily_unit_supply as

-- START significant sets block
with set_balance_changes as (
  -- mints
  select contract_address as set_address
    , value * 1.0 / 10 ^ 18 as quantity
    , evt_block_time as timestamp
  from setprotocol."SetToken_evt_Transfer"
  where "from" = '\x0000000000000000000000000000000000000000'
  union all

  -- redeems
  select contract_address as set_address
    , value * -1.0 / 10 ^ 18 as quantity
    , evt_block_time as timestamp
  from setprotocol."SetToken_evt_Transfer"
  where "to" = '\x0000000000000000000000000000000000000000'
)
, net_balance_changes as (
  select date_trunc('day', timestamp) as day
    , set_address
    , sum(quantity) as net_quantity_change
  from set_balance_changes
  group by 1,2
)
, significant_sets as (
    select set_address
    , sum(quantity) as current_balance
  from set_balance_changes
  group by 1
  having sum(quantity) > 10 -- filter out all sets that have less than 10 unit supply
)
-- END significant sets block 
, net_balance_changes_filtered as (
    select b.*
    from net_balance_changes b
    inner join significant_sets s on b.set_address = s.set_address
)
, day_series as (
  select generate_series(min(day), now(), '1 day') AS day
  from net_balance_changes_filtered
)
, daily_balances as (
  select d.day
    , nbc.set_address
    , sum(net_quantity_change) as supply
  from day_series d
  left join net_balance_changes_filtered nbc on nbc.day <= d.day
  group by 1,2
  order by 2,1
)
select * from daily_balances
;