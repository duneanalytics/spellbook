create or replace view setprotocol_v2.daily_tvl_raw as 

select dp.set_address
  , dp.component_token_address
  , dp.component_symbol
  , dp.day
  , dp.real_units_per_set_token
  , ds.supply as set_supply
  , dcp.price_usd
  , dcp.price_eth
from setprotocol_v2.view_daily_positions dp
inner join setprotocol_v2.view_daily_unit_supply ds
  on ds.set_address = dp.set_address
  and ds.day = dp.day
inner join setprotocol_V2.view_daily_component_prices dcp -- note that this depends on erc20.tokens
  on dcp.component_address = dp.component_token_address --TODO: standardize the naming
  and dcp.day = dp.day
  ;