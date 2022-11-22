-- grabs a snapshot of each set's position at midnight UTC of each day
create or replace view setprotocol_v2.view_daily_positions as 
with initial_components as (
  -- Get the initial components from the create function
  select output_0 as set_address
    , unnest(_components) as component_token_address
    , unnest(_units) as unit
    , call_block_time as timestamp
    , call_block_time::date as day
  from setprotocol_v2."SetTokenCreator_call_create"
  where call_success is true
)
-- TODO: Add component removals events as a zeroing out of the units
, position_changes as (
  -- initial components 
  select c.set_address
    , c.component_token_address
    , t.symbol as component_symbol
    , c.unit as raw_real_units
    , c.unit * 1.0 / 10 ^ coalesce(t.decimals, 18) as real_units_per_set_token
    , c.timestamp
    , c.day
  from initial_components c 
  left join erc20.tokens t on c.component_token_address = t.contract_address
  union
  -- subsequent component_level position changes
  select s."contract_address" as set_address
    , s."_component" as component_token_address
    , t.symbol as component_symbol
    , "_realUnit" as raw_real_units
    , "_realUnit" * 1.0 / 10 ^ coalesce(t.decimals, 18) as real_units_per_set_token
    , evt_block_time as timestamp
    , evt_block_time::date as day
  from setprotocol_v2."SetToken_evt_ExternalPositionUnitEdited" s
  left join erc20.tokens t on s."_component" = t.contract_address
  union
  select s."contract_address" as set_address
    , s."_component" as component_token_address
    , t.symbol as component_symbol
    , "_realUnit" as raw_real_units
    , "_realUnit" * 1.0 / 10 ^ coalesce(t.decimals, 18) as real_units_per_set_token
    , evt_block_time as timestamp
    , evt_block_time::date as day
  from setprotocol_v2."SetToken_evt_DefaultPositionUnitEdited" s
  left join erc20.tokens t on s."_component" = t.contract_address
)
, position_changes_ranked as (
  select set_address
    , component_token_address
    , component_symbol
    , raw_real_units
    , real_units_per_set_token
    , timestamp
    , day
    , row_number() over (partition by set_address, component_token_address, day order by timestamp desc) 
        as entry_num
        -- the last position of each day will have a entry_num of 1
    , lead(day, 1) over (partition by set_address, component_token_address order by timestamp) 
        as next_day
        -- this gives the day that this particular snapshot value is valid until
  from position_changes
  
)
, day_series as (
  SELECT generate_series(min(day), now(), '1 day') AS day 
        FROM position_changes
)
, daily_positions as (
  select pc.set_address
    , pc.component_token_address
    , pc.component_symbol
    , d.day
    , pc.raw_real_units
    , pc.real_units_per_set_token
  from day_series d
  left join position_changes_ranked pc
    on d.day >= pc.day 
    and d.day < coalesce(pc.next_day,now()::date) -- if it's missing that means it's the last entry in the series
    and entry_num = 1 -- get the last position change of each day
)
select * from daily_positions
;