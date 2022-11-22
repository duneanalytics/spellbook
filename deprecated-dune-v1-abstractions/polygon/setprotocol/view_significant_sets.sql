-- There's too many sets to deal with, so this query gets all sets that have a current balance of at least 10

create or replace view setprotocol.view_significant_sets as 
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
select *
from significant_sets
;