-- grabs a snapshot of each set's position multiplier at midnight UTC of each day
create or replace view setprotocol_v2.view_daily_position_multipliers as 

with position_multiplier_changes as (
  -- When the contract is created, assume the multiplier is 1
  select "_setToken" as set_address 
    , 10^18 as raw_multiplier
    , 1.0 as multiplier
    , evt_block_time as timestamp
    , evt_block_time::date as day
  from setprotocol_v2."Controller_evt_SetAdded"
  union all
  select "contract_address" as set_address
    , "_newMultiplier" as raw_multiplier
    , "_newMultiplier" * 1.0 / 10 ^ 18 as multiplier
    , evt_block_time as timestamp
    , evt_block_time::date as day
  from setprotocol_v2."SetToken_evt_PositionMultiplierEdited"
)
, position_multiplier_changes_ranked as (
  select set_address 
    , raw_multiplier
    , multiplier
    , timestamp
    , day
    , row_number() over (partition by set_address, day order by timestamp desc) 
        as entry_num
        -- the last position multiplier of each day will have a entry_num of 1
    , lead(day, 1) over (partition by set_address order by timestamp) 
        as next_day
        -- this gives the day that this particular snapshot value is valid until
  from position_multiplier_changes
)
, day_series as (
  SELECT generate_series(min(day), now(), '1 day') AS day 
        FROM position_multiplier_changes
)
, daily_position_multipliers as (
  select pmc.set_address
    , d.day
    , pmc.multiplier
  from day_series d
  left join position_multiplier_changes_ranked pmc 
    on d.day >= pmc.day 
    and d.day < coalesce(pmc.next_day,now()::date) -- if it's missing that means it's the last entry in the series
    and entry_num = 1 -- get the last position change of each day
)
select * from daily_position_multipliers;

