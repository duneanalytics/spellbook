-- grabs a snapshot of each set's position multiplier at midnight UTC of each day
create or replace view setprotocol_v2.view_daily_position_multipliers as 

with position_multiplier_changes as (
  -- TODO: Grab the contract creation date of each set and set its default multiplier to 1
  select "contract_address" as set_address
    , "_newMultiplier" as raw_multiplier
    , "_newMultiplier" * 1.0 / 10 ^ 18 as multiplier
    , evt_block_time as timestamp
    , evt_block_time::date as day
    , row_number() over (partition by "contract_address", evt_block_time::date order by evt_block_time desc) 
        as entry_num
        -- the last position multiplier of each day will have a entry_num of 1
    , lead(evt_block_time::date, 1) over (partition by "contract_address" order by evt_block_time) 
        as next_day
        -- this gives the day that this particular snapshot value is valid until
  from setprotocol_v2."SetToken_evt_PositionMultiplierEdited"
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
  left join position_multiplier_changes pmc 
    on d.day >= pmc.day 
    and d.day < coalesce(pmc.next_day,now()::date) -- if it's missing that means it's the last entry in the series
    and entry_num = 1 -- get the last position change of each day
)
select * from daily_position_multipliers;